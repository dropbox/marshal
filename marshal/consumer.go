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
	"sync"
	"sync/atomic"
	"time"

	"github.com/dropbox/kafka/proto"
)

// CommitToken is a minimal structure that contains only the information necessary to
// mark a message committed. This is done so that you can throw away the message instead
// of holding on to it in memory.
type CommitToken struct {
	topic  string
	partID int
	offset int64
}

// Message is a container for Kafka messages.
type Message proto.Message

// CommitToken returns a CommitToken for a message. This can be passed to the
// CommitByToken method.
func (m *Message) CommitToken() CommitToken {
	return CommitToken{
		topic:  m.Topic,
		partID: int(m.Partition),
		offset: m.Offset,
	}
}

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

	// ReleaseClaimsIfBehind indicates whether to release a claim if a consumer
	// is consuming at a rate slower than the partition is being produced to.
	ReleaseClaimsIfBehind bool

	// The maximum number of claims this Consumer is allowed to hold simultaneously.
	// MaximumClaims indicates the maximum number of partitions to be claimed when
	// ClaimEntireTopic is set to false. Otherwise, it indicates the maximum number
	// of topics to claim.
	// Set to 0 (default) to allow an unlimited number of claims.
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
	messages   chan *Message

	// claims maps partition IDs to claim structures. The lock protects read/write
	// access to this map.
	lock   sync.RWMutex
	claims map[string]map[int]*claim
	// caches the claimed topics
	claimedTopics   map[string]bool
	topicClaimsChan chan map[string]bool
}

// NewConsumer instantiates a consumer object for a given topic. You must create a
// separate consumer for every individual topic that you want to consume from. Please
// see the documentation on ConsumerBehavior.
func (m *Marshaler) NewConsumer(topicNames []string, options ConsumerOptions) (*Consumer, error) {
	if m.Terminated() {
		return nil, errors.New("Marshaler has terminated, no new consumers can be created")
	}

	if len(topicNames) > 1 && !options.ClaimEntireTopic {
		return nil, errors.New("ClaimEntireTopic must be set if provided more than one topic")
	} else if len(topicNames) == 0 {
		return nil, errors.New("must provide at least one topic")
	}

	partitions := make(map[string]int)

	for _, topic := range topicNames {
		partitions[topic] = m.Partitions(topic)
	}

	// Construct base structure
	c := &Consumer{
		alive:           new(int32),
		marshal:         m,
		topics:          topicNames,
		partitions:      partitions,
		options:         options,
		messages:        make(chan *Message, 10000),
		rand:            rand.New(rand.NewSource(time.Now().UnixNano())),
		claims:          make(map[string]map[int]*claim),
		claimedTopics:   make(map[string]bool),
		topicClaimsChan: make(chan map[string]bool, 1),
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

					// update topic claims
					if options.ClaimEntireTopic {
						if partID == 0 {
							c.claimedTopics[topic] = true
							c.safeUpdateTopicClaims(c.claimedTopics, true)
						}

						// don't fast re-claim partitions for a topic unless partition 0 is claimed
						if !c.claimedTopics[topic] {
							log.Infof("[%s:%d] blocked fast-reclaim because topic is not claimed",
								topic, partID)
							continue
						}
					}

					c.claims[topic][partID] = newClaim(
						topic, partID, c.marshal, c.messages, options)

				}
			}

			// this check needs to be after iterating all partitions in a topic
			if options.ClaimEntireTopic && len(c.claimedTopics) >= options.MaximumClaims {
				log.Infof("reached max-topics for fast-reclaim. Claimed topics: %v",
					c.claimedTopics)
				break
			}
		}
	}

	go c.manageClaims()
	return c, nil
}

// NewConsumerOptions returns a default set of options for the Consumer.
func NewConsumerOptions() ConsumerOptions {
	return ConsumerOptions{
		FastReclaim:           true,
		ClaimEntireTopic:      false,
		GreedyClaims:          false,
		StrictOrdering:        false,
		ReleaseClaimsIfBehind: true,
	}
}

func (c *Consumer) defaultTopic() string {
	if len(c.partitions) > 1 {
		log.Errorf("attempted to claim partitions for more than one topic")
		c.Terminate(false)
		return ""
	}

	for topic := range c.partitions {
		return topic
	}

	log.Errorf("couldn't find default topic!")
	c.Terminate(false)
	return ""
}

func (c *Consumer) defaultTopicPartitions() int {
	if len(c.partitions) > 1 {
		log.Errorf("attempted to claim partitions for more than one topic")
		c.Terminate(false)
		return 0
	}

	for _, partitions := range c.partitions {
		return partitions
	}

	log.Errorf("couldn't find default topic!")
	c.Terminate(false)
	return 0
}

// tryClaimPartition attempts to claim a partition and make it available in the consumption
// flow. If this is called a second time on a partition we already own, it will return
// false. Returns true only if the partition was never claimed and we succeeded in
// claiming it.
func (c *Consumer) tryClaimPartition(topic string, partID int) bool {

	if c.options.ClaimEntireTopic {
		if c.isTopicClaimLimitReached(topic) {
			return false
		}
	} else {
		if c.isClaimLimitReached() {
			return false
		}
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
				log.Errorf("Internal double-claim for %s:%d.", topic, partID)
				log.Errorf("This is a catastrophic error. We're terminating Marshal.")
				log.Errorf("No further messages will be available. Please restart.")
				c.marshal.PrintState()
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
		log.Errorf("attempted to claim partitions for more than a single topic")
		c.Terminate(false)
		return
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
			log.Infof("skipping unclaimed partition %s:%d because we previously released it",
				topic, partID)
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

// isTopicClaimLimitReached indicates whether we can claim any partition of this topic
// or not given the topics we've already claimed and MaximumClaims
func (c *Consumer) isTopicClaimLimitReached(topic string) bool {
	if c.options.MaximumClaims <= 0 {
		return false
	}

	c.lock.RLock()
	defer c.lock.RUnlock()
	claimed := make(map[string]bool)
	for topic, topicClaims := range c.claims {
		if claim, ok := topicClaims[0]; ok && claim.Claimed() {
			claimed[topic] = true
		}
	}

	if !claimed[topic] && len(claimed) >= c.options.MaximumClaims {
		return true
	}
	return false
}

// claimTopic attempts to claim the entire topic if we're in that mode. We use partition 0
// as the key, anybody who has that partition has claimed the entire topic. This requires all
// consumers to use this mode.
func (c *Consumer) claimTopics() {
	// let's make a copy of what's in c.claimedTopics
	latestClaims := make(map[string]bool)
	if !c.Terminated() {
		func() {
			c.lock.RLock()
			defer c.lock.RUnlock()
			for topic, claimed := range c.claimedTopics {
				latestClaims[topic] = claimed
			}
		}()
	}

	for topic, partitions := range c.partitions {
		if partitions <= 0 {
			delete(latestClaims, topic)
			c.safeUpdateTopicClaims(latestClaims, false)
			continue
		}

		// We use partition 0 as our "key". Whoever claims partition 0 is considered the owner of
		// the topic. See if partition 0 is claimed or not.
		lastClaim := c.marshal.GetLastPartitionClaim(topic, 0)
		if lastClaim.isClaimed(time.Now().Unix()) {
			// If it's not claimed by us, return.
			if lastClaim.GroupID != c.marshal.groupID ||
				lastClaim.ClientID != c.marshal.clientID {
				// in case we had this topic, but now somebody else has claimed it
				delete(latestClaims, topic)
				c.safeUpdateTopicClaims(latestClaims, false)
				continue
			}
			// topic has already been claimed. No need to send notification.
			latestClaims[topic] = true
		} else {
			// Unclaimed, so attempt to claim partition 0. This is how we key topic claims.
			log.Infof("[%s] attempting to claim topic (key partition 0)\n", topic)
			// we need to check if we're above the maximum topics to be claimed
			// we should only allow the first k topics to be claimed and allow all
			// of their partitions to be claimed. This is controlled by controlling how
			// many (key partition 0) we claim.

			if c.isTopicClaimLimitReached(topic) {
				log.Infof("blocked claiming topic: %s due to limit %d\n",
					topic, c.options.MaximumClaims)
				continue
			}

			if !c.tryClaimPartition(topic, 0) {
				continue
			}
			log.Infof("[%s] claimed topic (key partition 0) successfully\n", topic)
			latestClaims[topic] = true
			// a new partition claim, let's send topic claim notification if necessary
			c.safeUpdateTopicClaims(latestClaims, false)
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

// updatesTopicClaims if changed. It should be noted that it expects c.lock.RLock
// to be already acquired
func (c *Consumer) safeUpdateTopicClaims(latestClaims map[string]bool, force bool) {
	if !c.Terminated() {
		c.lock.RLock()
		defer c.lock.RUnlock()
		// let's compare the new topic claims vs old ones. Upon change, we should trigger an update
		// updateTopicClaims expects to be called with RLock held
		c.updateTopicClaims(latestClaims, force)
	}
}

// updatesTopicClaims if changed. It should be noted that it expects c.lock.RLock
// to be already acquired
func (c *Consumer) updateTopicClaims(latestClaims map[string]bool, force bool) {
	changed := false
	if force {
		changed = true
	} else {
		// check for missing topic claims
		for topic := range c.claimedTopics {
			if !latestClaims[topic] {
				changed = true
				break
			}
		}

		if !changed {
			// let's check if something in latest that is not in claimedTopics.
			// checking for length only is not sufficient
			for topic := range latestClaims {
				if !c.claimedTopics[topic] {
					changed = true
					break
				}
			}
		}
	}

	if changed {
		log.Infof("updating topic claims: %v\n", latestClaims)
		c.lock.RUnlock()
		defer c.lock.RLock()
		func() {
			// make sure that draining the channel and writing to it
			// happens in the same atomic operation
			c.lock.Lock()
			defer c.lock.Unlock()

			select {
			case <-c.topicClaimsChan:
			default:
			}

			// let's write a copy of the map because we need to differentiate in the future
			// between latestClaims and c.claimedTopics. We should also give out a new copy
			// to avoid having clients modify the claims unintentionally
			c.claimedTopics = make(map[string]bool, len(latestClaims))
			newClaims := make(map[string]bool, len(latestClaims))
			for topic, claimed := range latestClaims {
				c.claimedTopics[topic] = claimed
				newClaims[topic] = claimed
			}
			c.topicClaimsChan <- newClaims
		}()
	}
}

// updatePartitionCounts pulls the latest partition counts per topic from the Marshaler
func (c *Consumer) updatePartitionCounts() {
	for _, topic := range c.marshal.Topics() {
		// Only update partitions for topics we already know about
		if _, ok := c.partitions[topic]; ok {
			c.partitions[topic] = c.marshal.Partitions(topic)
		}
	}
}

// manageClaims is our internal state machine that handles partitions and claiming new
// ones (or releasing ones).
func (c *Consumer) manageClaims() {
	for !c.Terminated() {
		c.updatePartitionCounts()

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

// terminateAndCleanup instructs the consumer to commit its offsets,
// possibly release its partitions, and possibly remove its reference from
// the associated marshaler. This will allow other consumers to begin consuming.
func (c *Consumer) terminateAndCleanup(release bool, remove bool) bool {
	if !atomic.CompareAndSwapInt32(c.alive, 1, 0) {
		return false
	}

	latestTopicClaims := make(map[string]bool)
	releasedTopics := make(map[string]bool)
	c.lock.RLock()
	defer c.lock.RUnlock()

	for topic, topicClaims := range c.claims {
		for partID, claim := range topicClaims {
			if claim != nil {
				if release {
					claim.Release()
					if partID == 0 {
						releasedTopics[topic] = true
					}
				} else {
					claim.Terminate()
				}
			}
		}
	}

	close(c.messages)

	for topic := range c.claims {
		if !releasedTopics[topic] {
			latestTopicClaims[topic] = true
		}
	}

	// Optionally remove consumer from its marshal. Doing so is recommended
	// if the marshal doesn't explicitly remove the consumer.
	if remove {
		c.marshal.removeConsumer(c)
	}

	// update the claims
	// updateTopicClaims expects to be called with RLock held
	c.updateTopicClaims(latestTopicClaims, false)
	close(c.topicClaimsChan)
	return true

}

// Terminate instructs the consumer to clean up and allow other consumers to begin consuming.
// (If you do not call this method before exiting, things will still work, but more slowly.)
func (c *Consumer) Terminate(release bool) bool {
	return c.terminateAndCleanup(release, true)
}

// GetCurrentTopicClaims returns the topics that are currently claimed by this
// consumer. It should be relevent only when ClaimEntireTopic is set
func (c *Consumer) GetCurrentTopicClaims() (map[string]bool, error) {
	if !c.options.ClaimEntireTopic {
		err := fmt.Errorf("GetCurrentTopicClaims is only relevent when ClaimEntireTopic is set")
		log.Error(err.Error())
		return nil, err
	}

	c.lock.RLock()
	defer c.lock.RUnlock()

	// do a copy
	latestClaims := make(map[string]bool)
	for topic := range c.claimedTopics {
		latestClaims[topic] = true
	}

	return latestClaims, nil
}

// TopicClaims returns a read-only channel that receives updates for topic claims.
// It's only relevant when CLaimEntireTopic is set
func (c *Consumer) TopicClaims() <-chan map[string]bool {
	if !c.options.ClaimEntireTopic {
		err := fmt.Errorf("GetCurrentTopicClaims is only relevant when ClaimEntireTopic is set")
		log.Error(err.Error())
	}

	return c.topicClaimsChan
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
	// if we're claiming topics, then this is not applicable. It's handled inside claimTopics
	return !c.options.ClaimEntireTopic && c.options.MaximumClaims > 0 &&
		c.getNumActiveClaims() >= c.options.MaximumClaims
}

// ConsumeChannel returns a read-only channel. Messages that are retrieved from Kafka will be
// made available in this channel.
func (c *Consumer) ConsumeChannel() <-chan *Message {
	return c.messages
}

// consumeOne returns a single message. This is mostly used within the test suite to
// make testing easier as it simulates the message handling behavior.
func (c *Consumer) consumeOne() *Message {
	msg := <-c.messages
	c.Commit(msg)
	return msg
}

// Commit is called when you've finished processing a message. In the at-least-once
// consumption case, this will allow the "last processed offset" to move forward so that
// we can never see this message again. This operation does nothing for at-most-once
// consumption, as the commit happens in the Consume phase.
// TODO: AMO description is wrong.
func (c *Consumer) Commit(msg *Message) error {
	cl, ok := func() (*claim, bool) {
		c.lock.RLock()
		defer c.lock.RUnlock()

		cl, ok := c.claims[msg.Topic][int(msg.Partition)]
		return cl, ok
	}()
	if !ok {
		return errors.New("Message not committed (partition claim expired).")
	}
	return cl.Commit(msg.Offset)
}

// CommitByToken is called when you've finished processing a message. In the at-least-once
// consumption case, this will allow the "last processed offset" to move forward so that
// we can never see this message again. This particular method is used when you've only
// got a CommitToken to commit from.
func (c *Consumer) CommitByToken(token CommitToken) error {
	cl, ok := func() (*claim, bool) {
		c.lock.RLock()
		defer c.lock.RUnlock()

		cl, ok := c.claims[token.topic][token.partID]
		return cl, ok
	}()
	if !ok {
		return errors.New("Message not committed (partition claim expired).")
	}
	return cl.Commit(token.offset)
}

// PrintState outputs the status of the consumer.
func (c *Consumer) PrintState() {
	c.lock.RLock()
	defer c.lock.RUnlock()

	log.Infof("  CONSUMER: %d messages in queue", len(c.messages))
	for _, topic := range c.topics {
		log.Infof("    TOPIC: %s", topic)
		for _, claim := range c.claims[topic] {
			claim.PrintState()
		}
	}
}
