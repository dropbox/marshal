/*
 * portal - marshal
 *
 * a library that implements an algorithm for doing consumer coordination within Kafka, rather
 * than using Zookeeper or another external system.
 *
 */

package marshal

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dropbox/kafka"
	"github.com/dropbox/kafka/proto"
	"github.com/pborman/uuid"
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

// Marshaler is the coordinator type. It is designed to be used once per (client,
// group) and is thread safe. Creating one of these will create connections to your
// Kafka cluster and begin actively monitoring the coordination topic.
type Marshaler struct {
	// These members are not protected by the lock and can be read at any
	// time as they're write-once or only ever atomically updated. They must
	// never be overwritten once a Marshaler is created.
	quit        *int32
	cluster     *KafkaCluster
	ownsCluster bool
	instanceID  string
	clientID    string
	groupID     string
	offsets     kafka.OffsetCoordinator

	// Lock protects the following members; you must have this lock in order to
	// read from or write to these.
	lock      *sync.RWMutex
	consumers []*Consumer
}

// NewMarshaler connects to a cluster (given broker addresses) and prepares to handle marshalling
// requests. Given the way this system works, the marshaler has to process all messages in the
// topic before it's safely able to begin operating. This might take a while. NOTE: If you are
// creating multiple marshalers in your program, you should instead call Dial and then use
// the NewMarshaler method on that object.
func NewMarshaler(clientID, groupID string, brokers []string) (*Marshaler, error) {
	cluster, err := Dial("automatic", brokers, NewMarshalOptions())
	if err != nil {
		return nil, err
	}
	m, err := cluster.NewMarshaler(clientID, groupID)
	if err != nil {
		m.ownsCluster = true
	}
	return m, err
}

// newInstanceID creates a new random instance ID for use inside Marshal messages. This
// is generated new every time we restart.
func newInstanceID() string {
	// A UUID4 starts with 8 random characters, so let's use that as our instance ID.
	// This should be a good tradeoff between randomness and brevity.
	return uuid.New()[0:8]
}

// addNewConsumer is called when a new Consumer is created. This allows Marshal to keep
// track of the consumers that exist so we can operate on them later if needed.
func (m *Marshaler) addNewConsumer(c *Consumer) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.consumers = append(m.consumers, c)
}

// removeConsumer is called when a Consumer is terminating and should be removed from our list.
func (m *Marshaler) removeConsumer(c *Consumer) {
	m.lock.Lock()
	defer m.lock.Unlock()

	for i, cn := range m.consumers {
		if cn == c {
			m.consumers = append(m.consumers[:i], m.consumers[i+1:]...)
			break
		}
	}
}

// getClaimedPartitionState returns a topicState iff it is claimed by the current Marshaler.
// Else, an error is returned. This is on the Marshaler becomes it's a helper to only return
// a claim that is presently valid and owned by us.
func (m *Marshaler) getClaimedPartitionState(topicName string, partID int) (
	*topicState, error) {

	// Get partition state of whatever happens to be here
	topic := m.cluster.getPartitionState(m.groupID, topicName, partID)

	topic.lock.RLock()
	defer topic.lock.RUnlock()

	if !topic.partitions[partID].claimed(m.cluster.ts) {
		return nil, fmt.Errorf("Partition %s:%d is not claimed!", topicName, partID)
	}

	// And if it's not claimed by us...
	if topic.partitions[partID].GroupID != m.groupID ||
		topic.partitions[partID].ClientID != m.clientID {
		return nil, fmt.Errorf("Partition %s:%d is not claimed by us!", topicName, partID)
	}

	return topic, nil
}

// Topics returns the list of known topics.
func (m *Marshaler) Topics() []string {
	return m.cluster.getTopics()
}

// Partitions returns the count of how many partitions are in a given topic. Returns 0 if a
// topic is unknown.
func (m *Marshaler) Partitions(topicName string) int {
	return m.cluster.getTopicPartitions(topicName)
}

// terminateAndCleanup terminates the marshal, with the option of removing
// the marshaler's reference from its associated cluster.
func (m *Marshaler) terminateAndCleanup(remove bool) {
	if !atomic.CompareAndSwapInt32(m.quit, 0, 1) {
		return
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	// Now terminate all of the consumers. In this codepath we do a no-release termination
	// because that is usually correct in production. If someone actually wants to release
	// they need to terminate the consumers manually.
	for _, cn := range m.consumers {
		cn.terminateAndCleanup(false, false)
	}
	m.consumers = nil

	// If we own the cluster, terminate it.
	if m.ownsCluster {
		m.cluster.Terminate()
	}

	// Remove this marshal from its cluster. Doing so is recommended
	// if the cluster doesn't remove the terminated marshal itself (by setting its
	// list of marshals to nil or filtering them).
	if remove {
		m.cluster.removeMarshal(m)
	}
}

// Terminate is called when we're done with the marshaler and want to shut down.
func (m *Marshaler) Terminate() {
	m.terminateAndCleanup(true)
}

// Terminated returns whether or not we have been terminated.
func (m *Marshaler) Terminated() bool {
	return atomic.LoadInt32(m.quit) == 1
}

// Claimed returns the current status on whether or not a partition is claimed by any other
// consumer in our group (including ourselves). A topic/partition that does not exist is
// considered to be unclaimed.
func (m *Marshaler) Claimed(topicName string, partID int) bool {
	// The contract of this method is that if it returns something and the heartbeat is
	// non-zero, the partition is claimed.
	claim := m.GetPartitionClaim(topicName, partID)
	return claim.LastHeartbeat > 0
}

// GetPartitionClaim returns a PartitionClaim structure for a given partition. The structure
// describes the consumer that is currently claiming this partition. This is a copy of the
// claim structure, so changing it cannot change the world state.
func (m *Marshaler) GetPartitionClaim(topicName string, partID int) PartitionClaim {
	topic := m.cluster.getPartitionState(m.groupID, topicName, partID)

	topic.lock.RLock()
	defer topic.lock.RUnlock()

	if topic.partitions[partID].claimed(m.cluster.ts) {
		return topic.partitions[partID] // copy.
	}
	return PartitionClaim{}
}

// GetLastPartitionClaim returns a PartitionClaim structure for a given partition. The structure
// describes the consumer that is currently or most recently claiming this partition. This is a
// copy of the claim structure, so changing it cannot change the world state.
func (m *Marshaler) GetLastPartitionClaim(topicName string, partID int) PartitionClaim {
	topic := m.cluster.getPartitionState(m.groupID, topicName, partID)

	topic.lock.RLock()
	defer topic.lock.RUnlock()

	return topic.partitions[partID] // copy.
}

// GetPartitionOffsets returns the current state of a topic/partition. This has to hit Kafka
// thrice to ask about a partition, but it returns the full state of information that can be
// used to calculate consumer lag.
func (m *Marshaler) GetPartitionOffsets(topicName string, partID int) (PartitionOffsets, error) {
	var err error

	o := PartitionOffsets{}
	o.Earliest, err = m.cluster.broker.OffsetEarliest(topicName, int32(partID))
	if err != nil {
		return PartitionOffsets{}, err
	}

	o.Latest, err = m.cluster.broker.OffsetLatest(topicName, int32(partID))
	if err != nil {
		return PartitionOffsets{}, err
	}

	// Get committed offsets for our particular group using our offset coordinator.
	o.Committed, _, err = m.offsets.Offset(topicName, int32(partID))
	if err != nil {
		// This error happens when Kafka does not know about the partition i.e. no
		// offset has been committed here. In that case we ignore it.
		if err != proto.ErrUnknownTopicOrPartition {
			return PartitionOffsets{}, fmt.Errorf("offset fetch fail: %s", err)
		}
	}

	// Use the last claim we know about, whatever it is
	claim := m.GetLastPartitionClaim(topicName, partID)
	o.Current = claim.CurrentOffset
	return o, nil
}

// msgBase constructs a base message object for a message.
func (m *Marshaler) msgBase(topicName string, partID int) *msgBase {
	return &msgBase{
		Version:    1,
		Time:       int(time.Now().Unix()),
		InstanceID: m.instanceID,
		ClientID:   m.clientID,
		GroupID:    m.groupID,
		Topic:      topicName,
		PartID:     partID,
	}
}

// ClaimPartition is how you can actually claim a partition. If you call this, Marshal will
// attempt to claim the partition on your behalf. This is the low level function, you probably
// want to use a MarshaledConsumer. Returns a bool on whether or not the claim succeeded and
// whether you can continue.
func (m *Marshaler) ClaimPartition(topicName string, partID int) bool {
	topic := m.cluster.getPartitionState(m.groupID, topicName, partID)

	// Unlock is later, since this function might take a while
	// TODO: Move this logic to a func and defer the lock (for sanity sake)
	topic.lock.Lock()

	// If the topic is already claimed, we can short circuit the decision process
	if topic.partitions[partID].claimed(m.cluster.ts) {
		defer topic.lock.Unlock()
		if topic.partitions[partID].GroupID == m.groupID &&
			topic.partitions[partID].ClientID == m.clientID {
			return true
		}
		log.Warningf("Attempt to claim already claimed partition.")
		return false
	}

	// Make a channel for results, append it to the list so we hear about claims
	out := make(chan struct{}, 1)
	topic.partitions[partID].pendingClaims = append(
		topic.partitions[partID].pendingClaims, out)
	topic.lock.Unlock()

	// Produce message to kafka
	cl := &msgClaimingPartition{
		msgBase: *m.msgBase(topicName, partID),
	}
	_, err := m.cluster.producer.Produce(MarshalTopic, int32(topic.claimPartition),
		&proto.Message{Value: []byte(cl.Encode())})
	if err != nil {
		// If we failed to produce, this is probably serious so we should undo the work
		// we did and then return failure
		log.Errorf("Failed to produce to Kafka: %s", err)
		return false
	}

	// Wait for channel to close, which is the signal that the rationalizer has
	// updated the status.
	<-out

	// Now we have to check if we own the partition. If this returns anything, the partition
	// is ours. nil = not.
	topic, err = m.getClaimedPartitionState(topicName, partID)
	if topic == nil || err != nil {
		return false
	}
	return true
}

// Heartbeat will send an update for other people to know that we're still alive and
// still owning this partition. Returns an error if anything has gone wrong (at which
// point we can no longer assert we have the lock).
func (m *Marshaler) Heartbeat(topicName string, partID int, CurrentOffset int64) error {
	topic, err := m.getClaimedPartitionState(topicName, partID)
	if err != nil {
		return err
	}

	// All good, let's heartbeat
	cl := &msgHeartbeat{
		msgBase:       *m.msgBase(topicName, partID),
		CurrentOffset: CurrentOffset,
	}
	_, err = m.cluster.producer.Produce(MarshalTopic, int32(topic.claimPartition),
		&proto.Message{Value: []byte(cl.Encode())})
	if err != nil {
		return fmt.Errorf("Failed to produce heartbeat to Kafka: %s", err)
	}

	err = m.CommitOffsets(topicName, partID, CurrentOffset)
	return err
}

// ReleasePartition will send an update for other people to know that we're done with
// a partition. Returns an error if anything has gone wrong (at which
// point we can no longer assert we have the lock).
func (m *Marshaler) ReleasePartition(topicName string, partID int, CurrentOffset int64) error {
	topic, err := m.getClaimedPartitionState(topicName, partID)
	if err != nil {
		return err
	}

	// All good, let's release
	cl := &msgReleasingPartition{
		msgBase:       *m.msgBase(topicName, partID),
		CurrentOffset: CurrentOffset,
	}
	_, err = m.cluster.producer.Produce(MarshalTopic, int32(topic.claimPartition),
		&proto.Message{Value: []byte(cl.Encode())})
	if err != nil {
		return fmt.Errorf("Failed to produce release to Kafka: %s", err)
	}

	err = m.CommitOffsets(topicName, partID, CurrentOffset)
	return err
}

// CommitOffsets will commit the partition offsets to Kafka so it's available in the
// long-term storage of the offset coordination system. Note: this method does not ensure
// that this Marshal instance owns the topic/partition in question.
func (m *Marshaler) CommitOffsets(topicName string, partID int, CurrentOffset int64) error {
	err := m.offsets.Commit(topicName, int32(partID), CurrentOffset)
	if err != nil {
		// Do not count this as a returned error as that will cause us to drop consumption, but
		// do log it so people can see it
		log.Errorf("[%s:%d] failed to commit offsets: %s", topicName, partID, err)
	}
	return nil
}

// ClientID returns the client ID we're using
func (m *Marshaler) ClientID() string {
	return m.clientID
}

// GroupID returns the group ID we're using
func (m *Marshaler) GroupID() string {
	return m.groupID
}

// PrintState will take the current state of the Marshal world and print it verbosely to the
// logging output. This is used in the rare case where we're self-terminating or on request
// from the user.
func (m *Marshaler) PrintState() {
	m.lock.RLock()
	defer m.lock.RUnlock()

	m.cluster.lock.RLock()
	defer m.cluster.lock.RUnlock()

	log.Infof("Marshal state dump beginning.")
	log.Infof("")
	log.Infof("Group ID:    %s", m.groupID)
	log.Infof("Client ID:   %s", m.clientID)
	log.Infof("Instance ID: %s", m.instanceID)
	log.Infof("")
	log.Infof("Marshal topic partitions: %d", m.cluster.partitions)
	log.Infof("Known Kafka topics:       %d", len(m.cluster.topics))
	log.Infof("Internal rsteps counter:  %d", atomic.LoadInt32(m.cluster.rsteps))
	log.Infof("")
	log.Infof("State of the world:")
	log.Infof("")
	for group, topicmap := range m.cluster.groups {
		log.Infof("  GROUP: %s", group)
		for topic, state := range topicmap {
			log.Infof("    TOPIC: %s [on %s:%d]", topic, MarshalTopic, state.claimPartition)
			state.PrintState()
		}
	}
	log.Infof("")
	log.Infof("Consumer states:")
	log.Infof("")
	for _, consumer := range m.consumers {
		consumer.PrintState()
	}
	log.Infof("")
	log.Infof("Marshal state dump complete.")
}
