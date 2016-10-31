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
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dropbox/kafka"
	"github.com/dropbox/kafka/proto"
	"github.com/jpillora/backoff"
)

// int64slice is for sorting.
type int64slice []int64

func (a int64slice) Len() int           { return len(a) }
func (a int64slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a int64slice) Less(i, j int) bool { return a[i] < a[j] }

// claim is instantiated for each partition "claim" we have. This type is responsible for
// pulling data from Kafka and managing its cursors, heartbeating as necessary, and health
// checking itself.
type claim struct {
	// These items are read-only. They are never changed after the object is created,
	// so access to these may be done without the lock.
	topic  string
	partID int

	// lock protects all access to the member variables of this struct except for the
	// messages channel, which can be read from or written to without holding the lock.
	// Additionally the stopChan can be used.
	lock            *sync.RWMutex
	messagesLock    *sync.Mutex
	offsets         PartitionOffsets
	marshal         *Marshaler
	consumer        *Consumer
	rand            *rand.Rand
	terminated      *int32
	beatCounter     int32
	lastHeartbeat   int64
	lastMessageTime time.Time
	options         ConsumerOptions
	kafkaConsumer   kafka.Consumer
	messages        chan *Message
	stopChan        chan struct{}
	doneChan        chan struct{}

	// tracking is a dict that maintains information about offsets that have been
	// sent to and acknowledged by clients. An offset is inserted into this map when
	// we insert it into the message queue, and when it is committed we record an update
	// saying so. This map is pruned during the heartbeats.
	tracking            map[int64]bool
	outstandingMessages int

	// Number of heartbeat cycles this claim has been lagging, i.e., consumption is going
	// too slowly (defined as being behind by more than 2 heartbeat cycles)
	cyclesBehind int

	// History arrays used for calculating average velocity for health checking.
	offsetCurrentHistory [10]int64
	offsetLatestHistory  [10]int64
}

// newClaim returns an internal claim object, used by the consumer to manage the
// claim of a single partition. It is up to the caller to ensure healthCheckLoop gets
// called in a goroutine. If you do not, the claim will die from failing to heartbeat
// after a short period.
func newClaim(topic string, partID int, marshal *Marshaler, consumer *Consumer,
	messages chan *Message, options ConsumerOptions) *claim {

	// Get all available offset information
	offsets, err := marshal.GetPartitionOffsets(topic, partID)
	if err != nil {
		log.Errorf("[%s:%d] failed to get offsets: %s", topic, partID, err)
		return nil
	}
	log.Debugf("[%s:%d] consumer offsets: early = %d, cur/comm = %d/%d, late = %d",
		topic, partID, offsets.Earliest, offsets.Current, offsets.Committed, offsets.Latest)

	// For offsets, we strictly prefer the contents of the MarshalTopic and will use that
	// if present. If we don't have that data, then we'll fall back to the Kafka committed
	// offsets. Failing that we'll start at the beginning of the partition.
	if offsets.Current > 0 {
		// Ideal case, we just use the Marshal offset that is already set
	} else if offsets.Committed > 0 {
		log.Infof("[%s:%d] no Marshal offset found, using committed offset %d",
			topic, partID, offsets.Committed)
		offsets.Current = offsets.Committed
	} else {
		log.Infof("[%s:%d] no Marshal or committed offset found, using earliest offset %d",
			topic, partID, offsets.Earliest)
		offsets.Current = offsets.Earliest
	}

	// Construct object and set it up
	obj := &claim{
		lock:            &sync.RWMutex{},
		messagesLock:    &sync.Mutex{},
		stopChan:        make(chan struct{}),
		doneChan:        make(chan struct{}),
		marshal:         marshal,
		consumer:        consumer,
		topic:           topic,
		partID:          partID,
		terminated:      new(int32),
		offsets:         offsets,
		messages:        messages,
		options:         options,
		tracking:        make(map[int64]bool),
		rand:            rand.New(rand.NewSource(time.Now().UnixNano())),
		lastMessageTime: time.Now(),
	}

	// Now try to actually claim it, this can block a while
	log.Infof("[%s:%d] consumer attempting to claim", topic, partID)
	if !marshal.ClaimPartition(topic, partID) {
		log.Infof("[%s:%d] consumer failed to claim", topic, partID)
		return nil
	}

	// If that worked, kick off the main setup loop and return
	obj.setup()
	return obj
}

// setup is the initial worker that initializes the claim structure. Until this is done,
// our internal state is inconsistent.
func (c *claim) setup() {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Of course, if the current offset is greater than the earliest, we must reset
	// to the earliest known
	if c.offsets.Current < c.offsets.Earliest {
		log.Warningf("[%s:%d] consumer fast-forwarding from %d to %d",
			c.topic, c.partID, c.offsets.Current, c.offsets.Earliest)
		c.offsets.Current = c.offsets.Earliest
	}

	// Since it's claimed, we now want to heartbeat with the last seen offset
	err := c.marshal.Heartbeat(c.topic, c.partID, c.offsets.Current)
	if err != nil {
		log.Errorf("[%s:%d] consumer failed to heartbeat: %s", c.topic, c.partID, err)
		go c.Release()
		return
	}
	c.lastHeartbeat = time.Now().Unix()

	// Set up Kafka consumer
	consumerConf := kafka.NewConsumerConf(c.topic, int32(c.partID))
	consumerConf.StartOffset = c.offsets.Current
	consumerConf.MaxFetchSize = c.marshal.cluster.options.MaxMessageSize
	consumerConf.RequestTimeout = c.marshal.cluster.options.ConsumeRequestTimeout
	// Do not retry. If we get back no data, we'll do our own retries.
	consumerConf.RetryLimit = 0

	kafkaConsumer, err := c.marshal.cluster.broker.Consumer(consumerConf)
	if err != nil {
		log.Errorf("[%s:%d] consumer failed to create Kafka Consumer: %s",
			c.topic, c.partID, err)
		go c.Release()
		return
	}
	c.kafkaConsumer = kafkaConsumer

	// Start our maintenance goroutines that keep this system healthy
	go c.messagePump()

	// Totally done, let the world know and move on
	log.Infof("[%s:%d] consumer %s claimed at offset %d (is %d behind)",
		c.topic, c.partID, c.marshal.clientID, c.offsets.Current, c.offsets.Latest-c.offsets.Current)
}

// Commit is called by a Consumer class when the client has indicated that it has finished
// processing a message. This updates our tracking structure so the heartbeat knows how
// far ahead it can move our offset.
func (c *claim) Commit(offset int64) error {
	if c.Terminated() {
		return fmt.Errorf("[%s:%d] is no longer claimed; can't commit offset %d",
			c.topic, c.partID, offset)
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	_, ok := c.tracking[offset]
	if !ok {
		// This is bogus; committing an offset we've never seen?
		return fmt.Errorf("[%s:%d] committing offset %d but we've never seen it",
			c.topic, c.partID, offset)
	}
	c.tracking[offset] = true
	c.outstandingMessages--
	return nil
}

// Terminated returns whether the consumer has terminated the Claim. The claim may or may NOT
// remain claimed depending on whether it was released or not.
func (c *claim) Terminated() bool {
	return atomic.LoadInt32(c.terminated) == 1
}

// GetCurrentLag returns this partition's cursor lag.
func (c *claim) GetCurrentLag() int64 {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if c.offsets.Current < c.offsets.Latest {
		return c.offsets.Latest - c.offsets.Current
	}
	return 0
}

// Flush will write updated offsets to Kafka immediately if we have any outstanding offset
// updates to write. If not, this is a relatively quick no-op.
func (c *claim) Flush() error {
	// By definition a terminated claim has already flushed anything it can flush
	// or we've lost the lock so there's nothing we can do. It's not an error.
	if c.Terminated() {
		return nil
	}

	// This is technically a racey design, but the worst case is that we
	// will write out two correct heartbeats which is fine.
	didAdvance, currentOffset := c.updateCurrentOffsets()
	if !didAdvance {
		// Current offset did not advance
		return nil
	}

	// Now heartbeat this value and update our heartbeat time
	if err := c.marshal.Heartbeat(c.topic, c.partID, currentOffset); err != nil {
		go c.Release()
		return fmt.Errorf("[%s:%d] failed to flush, releasing: %s", c.topic, c.partID, err)
	}
	return nil
}

// Release will invoke commit offsets and release the Kafka partition. After calling Release,
// consumer cannot consume messages anymore.
// Does not return until the message pump has exited and the release has finished.
func (c *claim) Release() bool {
	return c.teardown(true)
}

// Terminate will invoke commit offsets, terminate the claim, but does NOT release the partition.
// Does not return until the message pump has exited and termination has finished.
func (c *claim) Terminate() bool {
	return c.teardown(false)
}

// teardown handles releasing the claim or just updating our offsets for a fast restart.
func (c *claim) teardown(releasePartition bool) bool {
	if !atomic.CompareAndSwapInt32(c.terminated, 0, 1) {
		<-c.doneChan
		return false
	}

	// Kill the stopchan now which is a useful way of knowing we're quitting within selects
	close(c.stopChan)

	// need to serialize access to the messages channel. We should not release if the message pump
	// is about to write to the consumer channel
	c.messagesLock.Lock()
	defer c.messagesLock.Unlock()

	// Let's update current offset internally to the last processed
	_, currentOffset := c.updateCurrentOffsets()

	// Advise the consumer that this claim is terminating, this is so that the consumer
	// can release other claims if we've lost part of a topic
	if c.consumer != nil {
		go c.consumer.claimTerminated(c, releasePartition)
	}

	var err error
	if releasePartition {
		log.Infof("[%s:%d] releasing partition claim", c.topic, c.partID)
		err = c.marshal.ReleasePartition(c.topic, c.partID, currentOffset)
	} else {
		// We're not releasing but we do want to update our offsets to the latest value
		// we know about, so issue a gratuitous heartbeat
		err = c.marshal.Heartbeat(c.topic, c.partID, currentOffset)
	}

	// Wait for messagePump to exit
	<-c.doneChan

	if err != nil {
		log.Errorf("[%s:%d] failed to release: %s", c.topic, c.partID, err)
		return false
	}
	return true
}

// messagePump continuously pulls message from Kafka for this partition and makes them
// available for consumption.
func (c *claim) messagePump() {
	// When the pump exits we close the doneChan so people can know when it's not
	// possible for the pump to be running
	defer close(c.doneChan)

	// This method MUST NOT make changes to the claim structure. Since we might
	// be running while someone else has the lock, and we can't get it ourselves, we are
	// forbidden to touch anything other than the consumer and the message channel.
	retry := &backoff.Backoff{Min: 10 * time.Millisecond, Max: 1 * time.Second, Jitter: true}
	for !c.Terminated() {
		msg, err := c.kafkaConsumer.Consume()
		if err == proto.ErrOffsetOutOfRange {
			// Fell out of range, presumably because we're handling this too slow, so
			// let's abandon this claim
			log.Warningf("[%s:%d] error consuming: out of range, abandoning partition",
				c.topic, c.partID)
			go c.Release()
			return
		} else if err == kafka.ErrNoData {
			// No data, just loop; if we're stuck receiving no data for too long the healthcheck
			// will start failing
			time.Sleep(retry.Duration())
			continue
		} else if err != nil {
			log.Errorf("[%s:%d] error consuming: %s", c.topic, c.partID, err)

			// Often a consumption error is caused by data going away, such as if we're consuming
			// from the head and Kafka has deleted the data. In that case we need to wait for
			// the next offset update, so let's not go crazy
			time.Sleep(1 * time.Second)
			continue
		}
		retry.Reset()

		// Briefly get the lock to update our tracking map... I wish there were
		// goroutine safe maps in Go.
		c.lock.Lock()
		c.lastMessageTime = time.Now()
		c.tracking[msg.Offset] = false
		c.outstandingMessages++
		c.lock.Unlock()

		// Push the message down to the client (this bypasses the Consumer)
		// We should NOT write to the consumer channel if the claim is no longer claimed. This
		// needs to be serialized with Release, otherwise a race-condition can potentially
		// lead to a write to a closed-channel. That's why we're using this lock. We're not
		// using the main lock to avoid deadlocks since the write to the channel is blocking
		// until someone consumes the message blocking all Commit operations.
		//
		// This must not block -- if we hold the messagesLock for too long we will cause
		// possible deadlocks.
		c.messagesLock.Lock()
		if !c.Terminated() {
			// This allocates a new Message to put the proto.Message in.
			// TODO: This is really annoying and probably stupidly inefficient, is there any
			// way to do this better?
			tmp := Message(*msg)
			select {
			case c.messages <- &tmp:
				// Message successfully delivered to queue
			case <-c.stopChan:
				// Claim is terminated, the message will go nowhere
			}
		}
		c.messagesLock.Unlock()
	}
	log.Debugf("[%s:%d] no longer claimed, pump exiting", c.topic, c.partID)
}

// heartbeat is the internal "send a heartbeat" function. Calling this will immediately
// send a heartbeat to Kafka. If we fail to send a heartbeat, we will release the
// partition.
func (c *claim) heartbeat() bool {
	// Unclaimed partitions don't heartbeat.
	if c.Terminated() {
		return false
	}

	// Lock held because we use c.offsets and update c.lastHeartbeat below
	c.lock.Lock()
	defer c.lock.Unlock()

	// Now heartbeat this value and update our heartbeat time
	err := c.marshal.Heartbeat(c.topic, c.partID, c.offsets.Current)
	if err != nil {
		log.Errorf("[%s:%d] failed to heartbeat, releasing: %s", c.topic, c.partID, err)
		go c.Release()
	}

	log.Infof("[%s:%d] heartbeat: Current offset is %d, partition offset range is %d..%d.",
		c.topic, c.partID, c.offsets.Current, c.offsets.Earliest, c.offsets.Latest)
	log.Infof("[%s:%d] heartbeat: There are %d messages in queue and %d messages outstanding.",
		c.topic, c.partID, len(c.messages), c.outstandingMessages)
	c.lastHeartbeat = time.Now().Unix()
	return true
}

// updateCurrentOffsets updates the current offsets so that a Commit/Heartbeat can pick up the
// latest offsets. Returns true if we advanced our current offset, false if there was no
// change. Also returns the latest current offset.
func (c *claim) updateCurrentOffsets() (bool, int64) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Get the sorted set of offsets
	offsets := make(int64slice, 0, len(c.tracking))
	for key := range c.tracking {
		offsets = append(offsets, key)
	}
	sort.Sort(offsets)

	// Now iterate the offsets bottom up and increment our current offset until we
	// see the first uncommitted offset (oldest message)
	didAdvance := false
	for _, offset := range offsets {
		if !c.tracking[offset] {
			break
		}
		// Remember current is always "last committed + 1", see the docs on
		// PartitionOffset for a reminder.
		didAdvance = true
		c.offsets.Current = offset + 1
		delete(c.tracking, offset)
	}

	// If we end up with more than a queue of outstanding messages, then something is
	// probably broken in the implementation... since that will cause us to grow
	// forever in memory, let's alert the user
	if len(c.tracking) > c.marshal.cluster.options.MaxMessageQueue {
		log.Errorf("[%s:%d] has %d uncommitted offsets. You must call Commit.",
			c.topic, c.partID, len(c.tracking))
	}
	return didAdvance, c.offsets.Current
}

// heartbeatExpired returns whether or not our last successful heartbeat is so
// long ago that we know we're expired.
func (c *claim) heartbeatExpired() bool {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.lastHeartbeat < time.Now().Unix()-HeartbeatInterval
}

// healthCheck performs a single health check against the claim. If we have failed
// too many times, this will also start a partition release. Returns true if the
// partition is healthy, else false.
func (c *claim) healthCheck() bool {
	// Unclaimed partitions aren't healthy.
	if c.Terminated() {
		return false
	}

	// Get velocities; these functions both use the locks so we have to do this before
	// we personally take the lock (to avoid deadlock)
	consumerVelocity := c.ConsumerVelocity()
	partitionVelocity := c.PartitionVelocity()

	// If our heartbeat is expired, we are definitely unhealthy... don't even bother
	// with checking velocity
	if c.heartbeatExpired() {
		log.Warningf("[%s:%d] consumer unhealthy by heartbeat test, releasing",
			c.topic, c.partID)
		go c.Release()
		return false
	}

	// If the consumer group owning this claim is paused, we must release this claim.
	if c.marshal.cluster.IsGroupPaused(c.marshal.GroupID()) {
		log.Infof("[%s:%d] consumer group %s is paused, claim releasing",
			c.topic, c.partID, c.marshal.GroupID())
		go c.Release()
		return false
	}

	// Take the lock below here as we are reading protected values on c and we're
	// writing to c.cyclesBehind
	c.lock.Lock()
	defer c.lock.Unlock()

	// If we haven't seen any messages for more than a heartbeat interval, it's possible
	// we've gotten into a bad state. Make a check to see how far behind we are, if we
	// are behind and not seeing any messages then release.
	if time.Now().After(c.lastMessageTime.Add(HeartbeatInterval * time.Second)) {
		if consumerVelocity == 0 && (partitionVelocity > 0 || c.offsets.Latest > c.offsets.Current) {
			// If that's true then it means velocity has been 0 for at least long enough
			// to drive the average to 0, which means about 10 heartbeat cycles. This is
			// long enough that releasing seems fine.
			log.Warningf("[%s:%d] no messages received for %d seconds with CV=%0.2f PV=%0.2f, releasing",
				c.topic, c.partID, HeartbeatInterval, consumerVelocity, partitionVelocity)
			go c.Release()
			return false
		} else {
			log.Infof("[%s:%d] no messages received for %d seconds with CV=%0.2f PV=%0.2f",
				c.topic, c.partID, HeartbeatInterval, consumerVelocity, partitionVelocity)
		}
	}

	// In topic claim mode we don't do any velocity checking. It's up to the consumer
	// to ensure they're claiming. TODO: Unclear if this is correct or not.
	if c.options.ClaimEntireTopic || !c.options.ReleaseClaimsIfBehind {
		return true
	}

	// We consider a consumer to be caught up if the predicted offset is past the end
	// of the partition. This takes into account the fact that we only get offset information
	// every hearbeat, so we could have some stale data.
	testOffset := c.offsets.Current + int64(consumerVelocity*2)
	if testOffset >= c.offsets.Latest {
		c.cyclesBehind = 0
		return true
	}

	// At this point we know the consumer is NOT PRESENTLY caught up or predicted to catch
	// up in the next two heartbeats.

	// If the consumer is moving as fast or faster than the partition and the consumer is
	// at least moving, consider it healthy. This case is true in the standard catching
	// up from behind case.
	if partitionVelocity < consumerVelocity {
		log.Infof("[%s:%d] consumer catching up: consume ∆ %0.2f >= produce ∆ %0.2f",
			c.topic, c.partID, consumerVelocity, partitionVelocity)
		c.cyclesBehind = 0
		return true
	}

	// Unhealthy, so increase the cycle count so we know when it's been unhealthy for
	// too long and we want to give it up
	c.cyclesBehind++

	// If were behind by too many cycles, then we should try to release the
	// partition. If so, do this in a goroutine since it will involve calling out
	// to Kafka and releasing the partition.
	if c.cyclesBehind >= 3 {
		log.Warningf("[%s:%d] consumer unhealthy for too long, releasing",
			c.topic, c.partID)
		go c.Release()
		return false
	}

	// Clearly we haven't been behind for long enough, so we're still "healthy"
	log.Warningf("[%s:%d] consumer too slow: consume ∆ %0.2f < produce ∆ %0.2f (warning #%d)",
		c.topic, c.partID, consumerVelocity, partitionVelocity, c.cyclesBehind)
	return true
}

// healthCheckLoop runs regularly and will perform a health check. Exits when this claim
// has been terminated.
func (c *claim) healthCheckLoop() {
	time.Sleep(<-c.marshal.cluster.jitters)
	for !c.Terminated() {
		// Attempt to update offsets; if this fails we want to to do a quicker retry
		// than the jitter interval to allow us to try to retry some times before we
		// give up.
		for !c.heartbeatExpired() {
			if err := c.updateOffsets(); err != nil {
				log.Errorf("[%s:%d] health check loop failed to update offsets: %s",
					c.topic, c.partID, err)
				time.Sleep(1 * time.Second)
				continue
			}
			break
		}

		// Now healthcheck and, if it's good, heartbeat
		if c.healthCheck() {
			go c.heartbeat()
		}
		time.Sleep(<-c.marshal.cluster.jitters)
	}
	log.Infof("[%s:%d] health check loop exiting, claim terminated",
		c.topic, c.partID)
}

// average returns the average of a given slice of int64s. It ignores 0s as
// those are "uninitialized" elements.
func average(vals []int64) float64 {
	min, max, ct := int64(0), int64(0), int64(0)
	for _, val := range vals {
		if val <= 0 {
			continue
		}
		if min == 0 || val < min {
			min = val
		}
		if max == 0 || val > max {
			max = val
		}
		ct++
	}

	if min == max || ct < 2 {
		return 0
	}

	return float64(max-min) / float64(ct-1)
}

// ConsumerVelocity returns the average of our consumers' velocity
func (c *claim) ConsumerVelocity() float64 {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return average(c.offsetCurrentHistory[0:])
}

// PartitionVelocity returns the average of the partition's velocity
func (c *claim) PartitionVelocity() float64 {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return average(c.offsetLatestHistory[0:])
}

// updateOffsets will update the offsets of our current partition.
func (c *claim) updateOffsets() error {
	// Start by updating our current offsets so even if we fail to get the offsets
	// we need to calculate Kafka data, we still move our current offset forward.
	c.updateCurrentOffsets()

	// Slow, hits Kafka. Run in a goroutine.
	offsets, err := c.marshal.GetPartitionOffsets(c.topic, c.partID)
	if err != nil {
		log.Errorf("[%s:%d] failed to get offsets: %s", c.topic, c.partID, err)
		return err
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	// Update the earliest/latest offsets that are presently available within the
	// partition
	c.offsets.Earliest = offsets.Earliest
	c.offsets.Latest = offsets.Latest

	// Do update our "history" values, this is used for calculating moving averages
	// in the health checking function
	c.offsetLatestHistory[c.beatCounter] = offsets.Latest
	c.offsetCurrentHistory[c.beatCounter] = c.offsets.Current

	c.beatCounter = (c.beatCounter + 1) % 10
	return nil
}

// numTrackingOffsets returns the size of the tracking dict.
func (c *claim) numTrackingOffsets() int {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return len(c.tracking)
}

// PrintState outputs the status of the consumer.
func (c *claim) PrintState() {
	c.lock.RLock()
	defer c.lock.RUnlock()

	// "Claimed" status is from Marshal rationalizer, and "Terminated" status is from
	// the local claim object (indicates we've exited somehow)
	state := "----"
	cl := c.marshal.GetPartitionClaim(c.topic, c.partID)
	if cl.Claimed() {
		if c.Terminated() {
			state = "CL+T"
		} else {
			state = "CLMD"
		}
	} else if c.Terminated() {
		state = "TERM"
	}

	ct := 0
	for _, st := range c.tracking {
		if st {
			ct++
		}
	}

	now := time.Now().Unix()

	log.Infof("      * %2d [%s]: offsets %d <= %d <= %d | %d",
		c.partID, state, c.offsets.Earliest, c.offsets.Current,
		c.offsets.Latest, c.offsets.Committed)
	log.Infof("                   BC %d | LHB %d (%d) | OM %d | CB %d",
		c.beatCounter, c.lastHeartbeat, now-c.lastHeartbeat,
		c.outstandingMessages, c.cyclesBehind)
	log.Infof("                   TRACK COMMITTED %d | TRACK OUTSTANDING %d",
		ct, len(c.tracking)-ct)
	log.Infof("                   PV %0.2f | CV %0.2f",
		c.PartitionVelocity(), c.ConsumerVelocity())
}
