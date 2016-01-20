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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/optiopay/kafka/proto"
)

// ConsumerOptions represents all of the options that a consumer can be configured with.
type ConsumerOptions struct {
	// FastReclaim instructs the consumer to attempt to reclaim any partitions
	// that are presently claimed by the ClientID/GroupID we have. This is useful
	// for situations where your ClientID is predictable/stable and you want to
	// minimize churn during restarts. This is dangerous if you have two copies
	// of your application running with the same ClientID/GroupID.
	// TODO: Create an instance ID for Marshaler such that we can detect when
	// someone else has decided to use our Client/Group.
	//
	// Note that this option ignores MaximumClaims, so it is possible to
	// exceed the claim limit if the ClientID previously held more claims.
	FastReclaim bool

	// ClaimEntireTopic makes Marshal handle claims on the entire topic rather than
	// on a per-partition basis. This is used with sharded produce/consume setups.
	// Defaults to false.
	ClaimEntireTopic bool

	// GreedyClaims indicates whether we should attempt to claim all unclaimed
	// partitions on start. This is appropriate in low QPS type environments.
	// Defaults to false/off.
	GreedyClaims bool

	// StrictOrdering tells the consumer that only a single message per partition
	// is allowed to be in-flight at a time. In order to consume the next message
	// you must commit the existing message. This option has a strong penalty to
	// consumption parallelism.
	StrictOrdering bool

	// The maximum number of claims this Consumer is allowed to hold simultaneously.
	// This limits the number of partitions even in the case of multiple topics.
	// Set to 0 (default) to allow an unlimited number of claims.ß
	//
	// Using this option will leave some partitions/topics completely unclaimed
	// if the number of Consumers in this GroupID falls below the number of
	// partitions/topics that exist.
	//
	// Note this limit does not apply to claims made via FastReclaim.
	MaximumClaims int
}

// Consumer allows you to safely consume data from a given topic in such a way that you
// don't need to worry about partitions and can safely split the load across as many
// processes as might be consuming from this topic. However, you should ONLY create one
// Consumer per topic in your application!
type Consumer struct {
	alive      *int32
	marshal    *Marshaler
	topics     []string
	partitions map[string]int
	rand       *rand.Rand
	options    ConsumerOptions
	messages   chan *proto.Message

	// claims maps partition IDs to claim structures. The lock protects read/write
	// access to this map.
	lock   sync.RWMutex
	claims map[string]map[int]*claim
}

// NewConsumer instantiates a consumer object for a given topic. You must create a
// separate consumer for every individual topic that you want to consume from. Please
// see the documentation on ConsumerBehavior.
func (m *Marshaler) NewConsumer(topicNames []string, options ConsumerOptions) (*Consumer, error) {
	if m.Terminated() {
		return nil, errors.New("Marshaler has terminated, no new consumers can be created")
	}

	if len(topicNames) > 1 && !options.ClaimEntireTopic {
		return nil, errors.New("ClaimEntireTopic must be set for more than one topic")
	} else if len(topicNames) == 0 {
		return nil, errors.New("length of topicNames must be postive")
	}

	partitions := make(map[string]int)

	for _, topic := range topicNames {
		partitions[topic] = m.Partitions(topic)
	}

	// Construct base structure
	c := &Consumer{
		alive:      new(int32),
		marshal:    m,
		topics:     topicNames,
		partitions: partitions,
		options:    options,
		messages:   make(chan *proto.Message, 10000),
		rand:       rand.New(rand.NewSource(time.Now().UnixNano())),
		claims:     make(map[string]map[int]*claim),
	}
	atomic.StoreInt32(c.alive, 1)
	m.addNewConsumer(c)

	// Fast-reclaim: iterate over existing claims in the given topics and see if
	// any of them look to be ours. Do this before the claim manager kicks off.
	if c.options.FastReclaim {
		for topic, partitionCount := range c.partitions {
			for partID := 0; partID < partitionCount; partID++ {
				cl := c.marshal.GetPartitionClaim(topic, partID)
				if cl.ClientID == c.marshal.ClientID() &&
					cl.GroupID == c.marshal.GroupID() {
					// This looks to be ours, let's do it. This is basically the fast path,
					// and our heartbeat will happen shortly from the automatic health
					// check which fires up immediately on newClaim.
					log.Infof("[%s:%d] attempting to fast-reclaim", topic, partID)
					if _, ok := c.claims[topic]; !ok {
						c.claims[topic] = make(map[int]*claim)
					}
					c.claims[topic][partID] = newClaim(
						topic, partID, c.marshal, c.messages, options)
				}
			}
		}
	}

	go c.manageClaims()
	return c, nil
}

// NewConsumerOptions returns a default set of options for the Consumer.
func NewConsumerOptions() ConsumerOptions {
	return ConsumerOptions{
		FastReclaim:      true,
		ClaimEntireTopic: false,
		GreedyClaims:     false,
		StrictOrdering:   false,
	}
}

func (c *Consumer) defaultTopic() string {
	if len(c.partitions) > 1 {
		log.Fatalf("attempted to claim partitions for more than one topic")
	}

	for topic, _ := range c.partitions {
		return topic
	}

	log.Fatalf("couldn't find default topic!")
	return ""
}

func (c *Consumer) defaultTopicPartitions() int {
	if len(c.partitions) > 1 {
		log.Fatalf("attempted to claim partitions for more than one topic")
	}

	for _, partitions := range c.partitions {
		return partitions
	}

	log.Fatalf("couldn't find default topic!")
	return 0
}

// tryClaimPartition attempts to claim a partition and make it available in the consumption
// flow. If this is called a second time on a partition we already own, it will return
// false. Returns true only if the partition was never claimed and we succeeded in
// claiming it.
func (c *Consumer) tryClaimPartition(topic string, partID int) bool {

	if c.isClaimLimitReached() {
		return false
	}

	// Partition unclaimed by us, see if it's claimed by anybody
	currentClaim := c.marshal.GetPartitionClaim(topic, partID)
	if currentClaim.LastHeartbeat > 0 {
		return false
	}

	// Set up internal claim structure we'll track things in, this can block for a while
	// as it talks to Kafka and waits for rationalizers.
	newClaim := newClaim(topic, partID, c.marshal, c.messages, c.options)
	if newClaim == nil {
		return false
	}

	// Critical section. Engage the lock here, we hold it until we exit.
	c.lock.Lock()
	defer c.lock.Unlock()

	// Ugh, we managed to claim a partition in our termination state. Don't worry too hard
	// and just release it.
	if c.Terminated() {
		// This can be a long blocking operation so send it to the background. We ultimately
		// don't care if it finishes or not, because the heartbeat will save us if we don't
		// submit a release message. This is just an optimization.
		go newClaim.Release()
		return false
	}

	// Ensure we don't have another valid claim in this slot. This shouldn't happen and if
	// it does we treat it as fatal. If this fires, it indicates with high likelihood there
	// is a bug in the Marshal state machine somewhere.
	topicClaims, ok := c.claims[topic]
	if ok {
		oldClaim, ok := topicClaims[partID]
		if ok && oldClaim != nil {
			if oldClaim.Claimed() {
				log.Fatalf("Internal double-claim for %s:%d.", topic, partID)
				log.Errorf("Internal double-claim for %s:%d.", topic, partID)
				log.Errorf("This is a catastrophic error. We're terminating Marshal.")
				log.Errorf("No further messages will be available. Please restart.")
				c.marshal.Terminate()
			}
		}
	}

	if _, ok := c.claims[topic]; !ok {
		c.claims[topic] = make(map[int]*claim)
	}

	// Save the claim, this makes it available for message consumption and status.
	c.claims[topic][partID] = newClaim
	return true
}

// rndIntn gets a random number.
func (c *Consumer) rndIntn(n int) int {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.rand.Intn(n)
}

// claimPartitions actually attempts to claim partitions. If the current consumer is
// set on aggressive, this will try to claim ALL partitions that are free. Balanced mode
// will claim a single partition.
func (c *Consumer) claimPartitions() {
	if len(c.partitions) > 1 {
		log.Fatalf("attempted to claim partitions for more than a single topic")
	}

	topic := c.defaultTopic()
	partitions := c.defaultTopicPartitions()
	if partitions <= 0 {
		return
	}
	// Don't bother trying to make claims if we are at our claim limit.
	// This is just an optimization, because we aren't holding the lock here
	// this check is repeated inside tryClaimPartition.
	if c.isClaimLimitReached() {
		return
	}

	offset := c.rndIntn(partitions)
	for i := 0; i < partitions; i++ {
		partID := (i + offset) % partitions

		// Get the most recent claim for this partition
		lastClaim := c.marshal.GetLastPartitionClaim(topic, partID)
		if lastClaim.isClaimed(time.Now().Unix()) {
			continue
		}

		// If the last claim was by this particular consumer, skip it; this is because
		// we might have become unhealthy and dropped it or we might already be
		// claiming this partition
		if lastClaim.GroupID == c.marshal.groupID &&
			lastClaim.ClientID == c.marshal.clientID {
			continue
		}

		// Unclaimed, so attempt to claim it
		if !c.tryClaimPartition(topic, partID) {
			continue
		}

		// If greedy claims is disabled, finish here
		if !c.options.GreedyClaims {
			break
		}
	}
}

// claimTopic attempts to claim the entire topic if we're in that mode. We use partition 0
// as the key, anybody who has that partition has claimed the entire topic. This requires all
// consumers to use this mode.
func (c *Consumer) claimTopics() {
	for topic, partitions := range c.partitions {
		if partitions <= 0 {
			continue
		}

		// We use partition 0 as our "key". Whoever claims partition 0 is considered the owner of
		// the topic. See if partition 0 is claimed or not.
		lastClaim := c.marshal.GetLastPartitionClaim(topic, 0)
		if lastClaim.isClaimed(time.Now().Unix()) {
			// If it's not claimed by us, return.
			if lastClaim.GroupID != c.marshal.groupID ||
				lastClaim.ClientID != c.marshal.clientID {
				continue
			}
		} else {
			// Unclaimed, so attempt to claim partition 0. This is how we key topic claims.
			log.Infof("[%s] attempting to claim topic (key partition 0)\n", topic)
			if !c.tryClaimPartition(topic, 0) {
				continue
			}
			log.Infof("[%s] claimed topic (key partition 0) successfully\n", topic)
		}

		// We either just claimed or we have already owned the 0th partition. Let's iterate
		// through all partitions and attempt to claim any that we don't own yet.
		for partID := 1; partID < partitions; partID++ {
			if !c.marshal.IsClaimed(topic, partID) {
				log.Infof("[%s:%d] claiming partition (topic claim mode)\n", topic, partID)
				c.tryClaimPartition(topic, partID)
			}
		}
	}

}

// manageClaims is our internal state machine that handles partitions and claiming new
// ones (or releasing ones).
func (c *Consumer) manageClaims() {
	for !c.Terminated() {
		// Attempt to claim more partitions, this always runs and will keep running until all
		// partitions in the topic are claimed (by somebody).
		if c.options.ClaimEntireTopic {
			c.claimTopics()
		} else {
			c.claimPartitions()
		}

		// Now sleep a bit so we don't pound things
		// TODO: Raise this later, we shouldn't attempt to claim this fast, this is just for
		// development.
		time.Sleep(time.Duration(c.rndIntn(3000)) * time.Millisecond)
	}
}

// Terminated returns whether or not this consumer has been terminated.
func (c *Consumer) Terminated() bool {
	return atomic.LoadInt32(c.alive) == 0
}

// Terminate instructs the consumer to commit its offsets and possibly release its partitions.
// This will allow other consumers to begin consuming.
// (If you do not call this method before exiting, things will still work, but more slowly.)
func (c *Consumer) Terminate(release bool) bool {
	if !atomic.CompareAndSwapInt32(c.alive, 1, 0) {
		return false
	}

	c.lock.RLock()
	defer c.lock.RUnlock()

	for _, topicClaims := range c.claims {
		for _, claim := range topicClaims {
			if claim != nil {
				if release {
					claim.Release()
				} else {
					claim.Terminate()
				}
			}
		}
	}

	close(c.messages)
	return true
}

// GetCurrentLag returns the number of messages that this consumer is lagging by. Note that
// this value can be unstable in the beginning of a run, as we might not have claimed all of
// partitions we will end up claiming, or we might have overclaimed and need to back off.
// Ideally this will settle towards 0. If it continues to rise, that implies there isn't
// enough consumer capacity.
func (c *Consumer) GetCurrentLag() int64 {
	c.lock.RLock()
	defer c.lock.RUnlock()

	var lag int64
	for _, topicClaims := range c.claims {
		for _, cl := range topicClaims {
			if cl.Claimed() {
				lag += cl.GetCurrentLag()
			}
		}
	}
	return lag
}

// GetCurrentLoad returns a number representing the "load" of this consumer. Think of this
// like a load average in Unix systems: the numbers are kind of related to how much work
// the system is doing, but by itself they don't tell you much.
func (c *Consumer) GetCurrentLoad() int {
	return c.getNumActiveClaims()
}

// getNumActiveClaims returns the number of claims actively owned by this Consumer.
func (c *Consumer) getNumActiveClaims() (ct int) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	for _, topicClaims := range c.claims {
		for _, cl := range topicClaims {
			if cl.Claimed() {
				ct++
			}
		}
	}
	return
}

// isClaimLimitReached returns the number of claims actively owned by this Consumer.
func (c *Consumer) isClaimLimitReached() bool {
	return c.options.MaximumClaims > 0 && c.getNumActiveClaims() >= c.options.MaximumClaims
}

// ConsumeChannel returns a read-only channel. Messages that are retrieved from Kafka will be
// made available in this channel.
func (c *Consumer) ConsumeChannel() <-chan *proto.Message {
	return c.messages
}

// consumeOne returns a single message. This is mostly used within the test suite to
// make testing easier as it simulates the message handling behavior.
func (c *Consumer) consumeOne() *proto.Message {
	msg := <-c.messages
	c.Commit(msg)
	return msg
}

// Commit is called when you've finished processing a message. In the at-least-once
// consumption case, this will allow the "last processed offset" to move forward so that
// we can never see this message again. This operation does nothing for at-most-once
// consumption, as the commit happens in the Consume phase.
// TODO: AMO description is wrong.
func (c *Consumer) Commit(msg *proto.Message) error {
	c.lock.RLock()
	defer c.lock.RUnlock()

	claim, ok := c.claims[msg.Topic][int(msg.Partition)]
	if !ok {
		return errors.New("Message not committed (partition claim expired).")
	}
	return claim.Commit(msg)
}
