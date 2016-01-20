/*
 * portal - marshal
 *
 * a library that implements an algorithm for doing consumer coordination within Kafka, rather
 * than using Zookeeper or another external system.
 *
 */

package marshal

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/optiopay/kafka"
)

const (
	// MarshalTopic is the main topic used for coordination. This must be constant across all
	// consumers that you want to coordinate.
	MarshalTopic = "__marshal"

	// HeartbeatInterval is the main timing used to determine how "chatty" the system is and how
	// fast it responds to failures of consumers. THIS VALUE MUST BE THE SAME BETWEEN ALL CONSUMERS
	// as it is critical to coordination.
	HeartbeatInterval = 60 // Measured in seconds.
)

// NewMarshaler connects to a cluster (given broker addresses) and prepares to handle marshalling
// requests. Given the way this system works, the marshaler has to process all messages in the
// topic before it's safely able to begin operating. This might take a while.
func NewMarshaler(clientID, groupID string, brokers []string) (*Marshaler, error) {
	// TODO: It might be nice to make the marshaler agnostic of clients and able to support
	// requests from N clients/groups. For now, though, we require instantiating a new
	// marshaler for every client/group.
	brokerConf := kafka.NewBrokerConf("PortalMarshal")
	brokerConf.Logger = &optiopayLoggerShim{l: log}
	broker, err := kafka.Dial(brokers, brokerConf)
	if err != nil {
		return nil, err
	}

	// Get offset coordinator so we can look up (and save) committed offsets later.
	coordinatorConf := kafka.NewOffsetCoordinatorConf(groupID)
	coordinator, err := broker.OffsetCoordinator(coordinatorConf)
	if err != nil {
		return nil, err
	}

	m := &Marshaler{
		quit:       new(int32),
		rsteps:     new(int32),
		instanceID: newInstanceID(),
		clientID:   clientID,
		groupID:    groupID,
		kafka:      broker,
		offsets:    coordinator,
		producer:   broker.Producer(kafka.NewProducerConf()),
		topics:     make(map[string]int),
		groups:     make(map[string]map[string]*topicState),
		jitters:    make(chan time.Duration, 100),
	}

	// Do an initial metadata fetch, this will block a bit
	err = m.refreshMetadata()
	if err != nil {
		return nil, fmt.Errorf("Failed to get metadata: %s", err)
	}

	// If there is no marshal topic, then we can't run. The admins must go create the topic
	// before they can use this library. Please see the README.
	m.partitions = m.Partitions(MarshalTopic)
	if m.partitions == 0 {
		return nil, errors.New("Marshalling topic not found. Please see the documentation.")
	}

	// Now we start a goroutine to start consuming each of the partitions in the marshal
	// topic. Note that this doesn't handle increasing the partition count on that topic
	// without stopping all consumers.
	m.rationalizers.Add(m.partitions)
	for id := 0; id < m.partitions; id++ {
		go m.rationalize(id, m.kafkaConsumerChannel(id))
	}

	// A jitter calculator, just fills a channel with random numbers so that other
	// people don't have to build their own random generator...
	go func() {
		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		for {
			jitter := rnd.Intn(HeartbeatInterval/2) + (HeartbeatInterval / 2)
			m.jitters <- time.Duration(jitter) * time.Second
		}
	}()

	// Now start the metadata refreshing goroutine
	go func() {
		for !m.Terminated() {
			time.Sleep(<-m.jitters)
			log.Infof("Refreshing topic metadata.")
			m.refreshMetadata()

			// See if the number of partitions in the marshal topic went up. If so, this is a
			// fatal error as it means we lose coordination. In theory a mass die-off of workers
			// is bad, but so is upsharding the coordination topic without shutting down
			// everything. At least this limits the damage horizon?
			if m.Partitions(MarshalTopic) != m.partitions {
				log.Fatalf("Marshal topic partition count changed. FATAL!")
			}
		}
	}()

	// Wait for all rationalizers to come alive
	log.Infof("Waiting for all rationalizers to come alive.")
	m.rationalizers.Wait()
	log.Infof("All rationalizers alive, Marshaler now alive.")

	return m, nil
}
