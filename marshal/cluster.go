/*
 * portal - marshal
 *
 * a library that implements an algorithm for doing consumer coordination within Kafka, rather
 * than using Zookeeper or another external system.
 *
 */

package marshal

import (
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dropbox/kafka"
)

// KafkaCluster is a user-agnostic view of the world. It connects to a Kafka cluster
// and runs rationalizers to observe the complete world state.
type KafkaCluster struct {
	// These members are not protected by the lock and can be read at any
	// time as they're write-once or only ever atomically updated. They must
	// never be overwritten once a KafkaCluster is created.
	quit       *int32
	name       string
	broker     *kafka.Broker
	producer   kafka.Producer
	partitions int
	jitters    chan time.Duration
	options    MarshalOptions

	// Lock protects the following members; you must have this lock in order to
	// read from or write to these.
	lock       *sync.RWMutex
	marshalers []*Marshaler
	topics     map[string]int
	groups     map[string]map[string]*topicState
	// pausedGroups stores the expiry time for groups that are paused.
	pausedGroups map[string]time.Time

	// This WaitGroup is used for signalling when all of the rationalizers have
	// finished processing.
	rationalizers *sync.WaitGroup

	// rsteps is updated whenever a rationalizer processes a log entry, this is
	// used mainly by the test suite.
	rsteps *int32

	// This is for testing only. When this is non-zero, the rationalizer will answer
	// queries based on THIS time instead of the current, actual time.
	ts int64
}

// MarshalOptions contains various tunables that can be used to adjust the configuration
// of the underlying system.
type MarshalOptions struct {
	// BrokerConnectionLimit is used to set the maximum simultaneous number of connections
	// that can be made to each broker.
	// Default: 30.
	BrokerConnectionLimit int

	// ConsumeRequestTimeout sets the time that we ask Kafka to wait before returning any
	// data to us. Setting this high uses more connections and can lead to some latency
	// but keeps the load on Kafka minimal. Use this to balance QPS against latency.
	//
	// Default: 1 millisecond.
	ConsumeRequestTimeout time.Duration

	// MarshalRequestTimeout is used for our coordination requests. This should be reasonable
	// at default, but is left as a tunable in case you have clients that are claiming an
	// extremely large number of partitions and are too slow. The overall Marshal latency
	// is impacted by this value as well as the MarshalRequestRetryWait below.
	//
	// Default: 1 millisecond.
	MarshalRequestTimeout time.Duration

	// MarshalRequestRetryWait is the time between consume requests Marshal generates. This
	// should be set to balance the above timeouts to prevent hammering the server.
	//
	// Default: 500 milliseconds.
	MarshalRequestRetryWait time.Duration

	// MaxMessageSize is the maximum size in bytes of messages that can be returned. This
	// must be set to the size of the largest messages your cluster is allowed to store,
	// else you will end up with stalled streams. I.e., Kafka will never send you a message
	// if the message is larger than this value but we can't detect that, we just think
	// there is no data.
	//
	// Default: 2,000,000 bytes.
	MaxMessageSize int32

	// MaxMessageQueue is the number of messages to retrieve from Kafka and store in-memory
	// waiting for consumption. This is per-Consumer and independent of message size so you
	// should adjust this for your consumption patterns.
	//
	// Default: 1000 messages.
	MaxMessageQueue int
}

// NewMarshalOptions returns a set of MarshalOptions populated with defaults.
func NewMarshalOptions() MarshalOptions {
	return MarshalOptions{
		BrokerConnectionLimit:   30,
		ConsumeRequestTimeout:   1 * time.Millisecond,
		MarshalRequestTimeout:   1 * time.Millisecond,
		MarshalRequestRetryWait: 500 * time.Millisecond,
		MaxMessageSize:          2000000,
		MaxMessageQueue:         1000,
	}
}

// Dial returns a new cluster object which can be used to instantiate a number of Marshalers
// that all use the same cluster. You may pass brokerConf or may set it to nil.
func Dial(name string, brokers []string, options MarshalOptions) (*KafkaCluster, error) {
	// Connect to Kafka
	brokerConf := kafka.NewBrokerConf("PortalMarshal")
	brokerConf.MetadataRefreshFrequency = time.Hour
	brokerConf.ConnectionLimit = options.BrokerConnectionLimit
	brokerConf.LeaderRetryLimit = 1 // Do not retry
	broker, err := kafka.Dial(brokers, brokerConf)
	if err != nil {
		return nil, err
	}

	c := &KafkaCluster{
		quit:          new(int32),
		rsteps:        new(int32),
		name:          name,
		options:       options,
		lock:          &sync.RWMutex{},
		rationalizers: &sync.WaitGroup{},
		broker:        broker,
		producer:      broker.Producer(kafka.NewProducerConf()),
		topics:        make(map[string]int),
		groups:        make(map[string]map[string]*topicState),
		pausedGroups:  make(map[string]time.Time),
		jitters:       make(chan time.Duration, 100),
		// It's important that marshalers begins as an empty slice and not nil to avoid
		// a race between NewMarshaler and Terminate. See note in Terminate.
		marshalers: make([]*Marshaler, 0),
	}

	// Do an initial metadata fetch, this will block a bit
	err = c.refreshMetadata()
	if err != nil {
		return nil, fmt.Errorf("Failed to get metadata: %s", err)
	}

	// If there is no marshal topic, then we can't run. The admins must go create the topic
	// before they can use this library. Please see the README.
	c.partitions = c.getTopicPartitions(MarshalTopic)
	if c.partitions == 0 {
		return nil, errors.New("Marshalling topic not found. Please see the documentation.")
	}

	// Now we start a goroutine to start consuming each of the partitions in the marshal
	// topic. Note that this doesn't handle increasing the partition count on that topic
	// without stopping all consumers.
	c.rationalizers.Add(c.partitions)
	for id := 0; id < c.partitions; id++ {
		go c.rationalize(id, c.kafkaConsumerChannel(id))
	}

	// A jitter calculator, just fills a channel with random numbers so that other
	// people don't have to build their own random generator. It is important that
	// these values be somewhat less than the HeartbeatInterval as we use this for
	// jittering our heartbeats.
	go func() {
		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		for {
			jitter := rnd.Intn(HeartbeatInterval/2) + (HeartbeatInterval / 4)
			c.jitters <- time.Duration(jitter) * time.Second
		}
	}()

	// Now start the metadata refreshing goroutine
	go func() {
		for !c.Terminated() {
			time.Sleep(<-c.jitters)
			log.Infof("[%s] Refreshing topic metadata.", c.name)
			c.refreshMetadata()

			// See if the number of partitions in the marshal topic changed. This is bad if
			// it happens, since it means we can no longer coordinate correctly.
			if c.getTopicPartitions(MarshalTopic) != c.partitions {
				log.Errorf("[%s] Marshal topic partition count changed. Terminating!", c.name)
				c.Terminate()
			}
		}
	}()

	// Wait for all rationalizers to come alive
	log.Infof("[%s] Waiting for all rationalizers to come alive.", c.name)
	c.rationalizers.Wait()
	log.Infof("[%s] All rationalizers alive, KafkaCluster now alive.", c.name)

	return c, nil
}

// NewMarshaler creates a Marshaler off of an existing cluster. This is more efficient
// if you're creating multiple instances, since they can share the same underlying cluster.
func (c *KafkaCluster) NewMarshaler(clientID, groupID string) (*Marshaler, error) {
	if c.Terminated() {
		return nil, errors.New("Cluster is terminated.")
	}

	// Get offset coordinator so we can look up (and save) committed offsets later.
	coordinator, err := c.getOffsetCoordinator(groupID)
	if err != nil {
		return nil, err
	}

	m := &Marshaler{
		quit:       new(int32),
		cluster:    c,
		instanceID: newInstanceID(),
		clientID:   clientID,
		groupID:    groupID,
		offsets:    coordinator,
		lock:       &sync.RWMutex{},
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	// This is a bit of hack, see note in KafkaCluster::Terminate.
	if c.marshalers == nil {
		return nil, errors.New("Cluster is terminated (marshalers is nil).")
	}

	// Remove any dead marshalers from our slice and add the new one
	filtered := make([]*Marshaler, 0)
	for _, marshaler := range c.marshalers {
		if !marshaler.Terminated() {
			filtered = append(filtered, marshaler)
		}
	}
	filtered = append(filtered, m)
	c.marshalers = filtered

	return m, nil
}

// refreshMetadata is periodically used to update our internal state with topic information
// about the world.
func (c *KafkaCluster) refreshMetadata() error {
	md, err := c.broker.Metadata()
	if err != nil {
		return err
	}

	newTopics := make(map[string]int)
	for _, topic := range md.Topics {
		newTopics[topic.Name] = len(topic.Partitions)
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	c.topics = newTopics
	return nil
}

// getOffsetCoordinator returns a kafka.OffsetCoordinator for a specific group.
func (c *KafkaCluster) getOffsetCoordinator(groupID string) (kafka.OffsetCoordinator, error) {
	return c.broker.OffsetCoordinator(
		kafka.NewOffsetCoordinatorConf(groupID))
}

// getClaimPartition calculates which partition a topic should use for coordination. This uses
// a hashing function (non-cryptographic) to predictably partition the topic space.
func (c *KafkaCluster) getClaimPartition(topicName string) int {
	// We use MD5 because it's a fast and good hashing algorithm and we don't need cryptographic
	// properties. We then take the first 8 bytes and treat them as a uint64 and modulo that
	// across how many partitions we have.
	hash := md5.Sum([]byte(topicName))
	uval := binary.LittleEndian.Uint64(hash[0:8])
	return int(uval % uint64(c.partitions))
}

// getGroupState returns the map of topics to topicState objects for a group.
func (c *KafkaCluster) getGroupState(groupID string) map[string]*topicState {
	// Read lock check
	c.lock.RLock()
	if group, ok := c.groups[groupID]; ok {
		c.lock.RUnlock()
		return group
	}
	c.lock.RUnlock()

	// Failed, write lock check and possible create
	c.lock.Lock()
	defer c.lock.Unlock()

	if group, ok := c.groups[groupID]; ok {
		return group
	}
	c.groups[groupID] = make(map[string]*topicState)
	return c.groups[groupID]
}

// getTopicState returns a topicState for a given topic.
func (c *KafkaCluster) getTopicState(groupID, topicName string) *topicState {
	group := c.getGroupState(groupID)

	// Read lock check
	c.lock.RLock()
	if topic, ok := group[topicName]; ok {
		c.lock.RUnlock()
		return topic
	}
	c.lock.RUnlock()

	// Write lock check and possible create
	c.lock.Lock()
	defer c.lock.Unlock()

	if topic, ok := group[topicName]; ok {
		return topic
	}
	group[topicName] = &topicState{
		claimPartition: c.getClaimPartition(topicName),
		partitions:     nil,
		lock:           &sync.RWMutex{},
	}
	return group[topicName]
}

// getPartitionState returns a topicState and possibly creates it and the partition state within
// the State.
func (c *KafkaCluster) getPartitionState(groupID, topicName string, partID int) *topicState {
	// Get topic and lock it so we can update it if needed
	topic := c.getTopicState(groupID, topicName)

	// Read lock check
	topic.lock.RLock()
	if len(topic.partitions) > partID {
		topic.lock.RUnlock()
		return topic
	}
	topic.lock.RUnlock()

	// Must upgrade, looks like we need a new partition
	topic.lock.Lock()
	defer topic.lock.Unlock()

	if len(topic.partitions) < partID+1 {
		for i := len(topic.partitions); i <= partID; i++ {
			topic.partitions = append(topic.partitions, PartitionClaim{})
		}
	}
	return topic
}

// getTopics returns the list of known topics.
func (c *KafkaCluster) getTopics() []string {
	c.lock.RLock()
	defer c.lock.RUnlock()

	topics := make([]string, 0, len(c.topics))
	for topic := range c.topics {
		topics = append(topics, topic)
	}
	return topics
}

// getTopicPartitions returns the count of how many partitions are in a given topic. Returns 0 if a
// topic is unknown.
func (c *KafkaCluster) getTopicPartitions(topicName string) int {
	c.lock.RLock()
	defer c.lock.RUnlock()

	count, _ := c.topics[topicName]
	return count
}

// removeMarshal removes a terminated Marshal from a cluster's list.
func (c *KafkaCluster) removeMarshal(m *Marshaler) {
	c.lock.Lock()
	defer c.lock.Unlock()

	for i, ml := range c.marshalers {
		if ml == m {
			c.marshalers = append(c.marshalers[:i], c.marshalers[i+1:]...)
			break
		}
	}
}

// waitForRsteps is used by the test suite to ask the rationalizer to wait until some number
// of events have been processed. This also returns the current rsteps when it returns.
func (c *KafkaCluster) waitForRsteps(steps int) int {
	for {
		cval := atomic.LoadInt32(c.rsteps)
		if cval >= int32(steps) {
			return int(cval)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// pauseConsumerGroup stores an expiry time for consumer groups that we'd like to pause.
func (c *KafkaCluster) pauseConsumerGroup(groupID string, adminID string, expiry time.Time) {
	c.lock.Lock()
	defer c.lock.Unlock()

	log.Warningf("Cluster marking group %s paused with expiry: %s", groupID, expiry.Format(time.UnixDate))
	c.pausedGroups[groupID] = expiry
}

// IsGroupPaused returns true if the given consumer group is paused.
// TODO(pihu) This just checks the expiry time, and not the admin ID.
func (c *KafkaCluster) IsGroupPaused(groupID string) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if res, ok := c.pausedGroups[groupID]; !ok {
		return false
	} else {
		return time.Now().Before(res)
	}
}

// Terminate is called when we're done with the marshaler and want to shut down.
func (c *KafkaCluster) Terminate() {
	if !atomic.CompareAndSwapInt32(c.quit, 0, 1) {
		return
	}

	log.Infof("[%s] beginning termination", c.name)

	// This is a bit of a hack, but because marshaler.terminateAndCleanup requires the read lock
	// on c, we can't terminate the Marshalers in the list while holding the write lock.
	// Because KafkaCluster::NewMarshaler will return an error if the marshalers slice is nil,
	// we know there cannot be new Marshalers created which aren't included in the local slice
	// we create here.
	//
	// There is probably some alternative where the quit variable is protected by the mutex instead
	// of being atomic, but this seems somewhat cleaner in case some future refactoring eliminates
	// the marshalers slice entirely this hack will go away automatically.
	c.lock.Lock()
	marshalers := c.marshalers
	c.marshalers = nil
	c.lock.Unlock()

	// Terminate all Marshalers which will in turn terminate all Consumers and
	// let everybody know we're all done.
	for _, marshaler := range marshalers {
		marshaler.terminateAndCleanup(false)
	}

	// Close the broker asynchronously to prevent blocking on potential network I/O
	go c.broker.Close()
}

// Terminated returns whether or not we have been terminated.
func (c *KafkaCluster) Terminated() bool {
	return atomic.LoadInt32(c.quit) == 1
}
