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

	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
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
	lock          *sync.RWMutex
	messagesLock  *sync.Mutex
	offsets       PartitionOffsets
	marshal       *Marshaler
	rand          *rand.Rand
	claimed       *int32
	terminated    *int32
	beatCounter   int32
	lastHeartbeat int64
	options       ConsumerOptions
	consumer      kafka.Consumer
	messages      chan *proto.Message

	// tracking is a dict that maintains information about offsets that have been
	// sent to and acknowledged by clients. An offset is inserted into this map when
	// we insert it into the message queue, and when it is committed we record an update
	// saying so. This map is pruned during the heartbeats.
	tracking               map[int64]bool
	outstandingMessages    int
	outstandingMessageWait *sync.Cond

	// Number of heartbeat cycles this claim has been lagging, i.e., consumption is going
	// too slowly (defined as being behind by more than 2 heartbeat cycles)
	cyclesBehind int

	// History arrays used for calculating average velocity for health checking.
	offsetCurrentHistory [10]int64
	offsetLatestHistory  [10]int64
}

// newClaim returns an internal claim object, used by the consumer to manage the
// claim of a single partition.
func newClaim(topic string, partID int, marshal *Marshaler,
	messages chan *proto.Message, options ConsumerOptions) *claim {
	// Get all available offset information
	offsets, err := marshal.GetPartitionOffsets(topic, partID)
	if err != nil {
		log.Errorf("[%s:%d] failed to get offsets: %s", topic, partID, err)
		return nil
	}
	log.Debugf("[%s:%d] consumer offsets: early = %d, cur/comm = %d/%d, late = %d",
		topic, partID, offsets.Earliest, offsets.Current, offsets.Committed, offsets.Latest)

	// Take the greatest of the committed/current offset, this means we will recover from
	// a situation where the last time this partition was claimed has fallen out of our
	// coordination memory.
	if offsets.Committed > 0 && offsets.Committed > offsets.Current {
		if offsets.Current > 0 {
			// This state shouldn't really happen. This implies that someone went back in time
			// and started consuming earlier than the committed offset.
			log.Warningf("[%s:%d] committed offset is %d but heartbeat offset was %d",
				topic, partID, offsets.Committed, offsets.Current)
		} else {
			log.Infof("[%s:%d] recovering committed offset of %d",
				topic, partID, offsets.Committed)
		}
		offsets.Current = offsets.Committed
	}

	// Construct object and set it up
	obj := &claim{
		lock:         &sync.RWMutex{},
		messagesLock: &sync.Mutex{},
		marshal:      marshal,
		topic:        topic,
		partID:       partID,
		claimed:      new(int32),
		terminated:   new(int32),
		offsets:      offsets,
		messages:     messages,
		options:      options,
		tracking:     make(map[int64]bool),
		rand:         rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	atomic.StoreInt32(obj.claimed, 1)
	obj.outstandingMessageWait = sync.NewCond(obj.lock)

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
		atomic.StoreInt32(c.claimed, 0)
		return
	}
	c.lastHeartbeat = time.Now().Unix()

	// Set up Kafka consumer
	consumerConf := kafka.NewConsumerConf(c.topic, int32(c.partID))
	consumerConf.StartOffset = c.offsets.Current
	kafkaConsumer, err := c.marshal.kafka.Consumer(consumerConf)
	if err != nil {
		log.Errorf("[%s:%d] consumer failed to create Kafka Consumer: %s",
			c.topic, c.partID, err)
		// TODO: There is an optimization here where we could release the partition.
		// As it stands, we're not doing anything,
		atomic.StoreInt32(c.claimed, 0)
		return
	}
	c.consumer = kafkaConsumer

	// Start our maintenance goroutines that keep this system healthy
	go c.healthCheckLoop()
	go c.messagePump()

	// Totally done, let the world know and move on
	log.Infof("[%s:%d] consumer claimed at offset %d (is %d behind)",
		c.topic, c.partID, c.offsets.Current, c.offsets.Latest-c.offsets.Current)
}

// Commit is called by a Consumer class when the client has indicated that it has finished
// processing a message. This updates our tracking structure so the heartbeat knows how
// far ahead it can move our offset.
func (c *claim) Commit(msg *proto.Message) error {
	if atomic.LoadInt32(c.claimed) != 1 {
		return fmt.Errorf("[%s:%d] is no longer claimed; can't commit offset %d",
			c.topic, c.partID, msg.Offset)
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	_, ok := c.tracking[msg.Offset]
	if !ok {
		// This is bogus; committing an offset we've never seen?
		return fmt.Errorf("[%s:%d] committing offset %d but we've never seen it",
			c.topic, c.partID, msg.Offset)
	}
	c.tracking[msg.Offset] = true
	c.outstandingMessages--
	// And now if we're using strict ordering mode, we can wake up the messagePump to
	// advise it that it's ready to continue with its life
	if c.options.StrictOrdering {
		if c.outstandingMessages == 0 {
			defer c.outstandingMessageWait.Signal()
		}
	}
	return nil
}

// Claimed returns whether or not this claim structure is alive and well and believes
// that it is still an active claim.
func (c *claim) Claimed() bool {
	return atomic.LoadInt32(c.claimed) == 1
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

// Release will invoke commit offsets and release the Kafka partition. After calling Release,
// consumer cannot consume messages anymore
func (c *claim) Release() bool {
	return c.teardown(true)
}

// Terminate will invoke commit offsets, terminate the claim, but does NOT release the partition.
// After calling Release, consumer cannot consume messages anymore
func (c *claim) Terminate() bool {
	return c.teardown(false)
}

// internal function that will teardown the claim. It may release the partition or
func (c *claim) teardown(releasePartition bool) bool {
	if !atomic.CompareAndSwapInt32(c.terminated, 0, 1) {
		return false
	}

	// need to serialize access to the messages channel. We should not release if the message pump
	// is about to write to the consumer channel
	c.messagesLock.Lock()
	defer c.messagesLock.Unlock()

	// Let's update current offset internally to the last processed
	c.updateCurrentOffsets()

	// Holds the lock through a Kafka transaction, but since we're releasing I think this
	// is reasonable. Held because of using offsetCurrent below.
	c.lock.RLock()
	defer c.lock.RUnlock()
	var err error
	if releasePartition {
		log.Infof("[%s:%d] releasing partition claim", c.topic, c.partID)
		atomic.StoreInt32(c.claimed, 0)
		err = c.marshal.ReleasePartition(c.topic, c.partID, c.offsets.Current)
	} else {
		err = c.marshal.CommitOffsets(c.topic, c.partID, c.offsets.Current)
	}

	if err != nil {
		log.Errorf("[%s:%d] failed to release: %s", c.topic, c.partID, err)
		return false
	}
	return true
}

// messagePump continuously pulls message from Kafka for this partition and makes them
// available for consumption.
func (c *claim) messagePump() {
	// This method MUST NOT make changes to the claim structure. Since we might
	// be running while someone else has the lock, and we can't get it ourselves, we are
	// forbidden to touch anything other than the consumer and the message channel.
	for c.Claimed() {
		msg, err := c.consumer.Consume()
		if err == proto.ErrOffsetOutOfRange {
			// Fell out of range, presumably because we're handling this too slow, so
			// let's abandon this consumer
			log.Warningf("[%s:%d] error consuming: out of range, abandoning partition",
				c.topic, c.partID)
			go c.Release()
			return
		} else if err != nil {
			log.Errorf("[%s:%d] error consuming: %s", c.topic, c.partID, err)

			// Often a consumption error is caused by data going away, such as if we're consuming
			// from the head and Kafka has deleted the data. In that case we need to wait for
			// the next offset update, so let's not go crazy
			time.Sleep(1 * time.Second)
			continue
		}

		// Briefly get the lock to update our tracking map... I wish there were
		// goroutine safe maps in Go.
		c.lock.Lock()
		if c.options.StrictOrdering {
			// Wait for there to be no outstanding messages so we can guarantee the
			// ordering.
			for c.outstandingMessages > 0 {
				c.outstandingMessageWait.Wait()
			}
		}
		c.tracking[msg.Offset] = false
		c.outstandingMessages++
		c.lock.Unlock()

		// Push the message down to the client (this bypasses the Consumer)
		// We should NOT write to the consumer channel if the claim is no longer claimed. This
		// needs to be serialized with Release, otherwise a race-condition can potentially
		// lead to a write to a closed-channel. That's why we're using this lock. We're not
		// using the main lock to avoid deadlocks since the write to the channel is blocking
		// until someone consumes the message blocking all Commit operations
		c.messagesLock.Lock()
		if !c.Terminated() {
			c.messages <- msg
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
	if atomic.LoadInt32(c.claimed) != 1 {
		return false
	}

	// Let's update current offset internally to the last processed
	c.updateOffsets()

	// Lock held because we use c.offsets and update c.lastHeartbeat below
	c.lock.Lock()
	defer c.lock.Unlock()

	// Now heartbeat this value and update our heartbeat time
	err := c.marshal.Heartbeat(c.topic, c.partID, c.offsets.Current)
	if err != nil {
		log.Errorf("[%s:%d] failed to heartbeat, releasing: %s", c.topic, c.partID, err)
		go c.Release()
	}

	log.Infof("[%s:%d] heartbeat: current offset %d with %d uncommitted messages",
		c.topic, c.partID, c.offsets.Current, c.outstandingMessages)
	c.lastHeartbeat = time.Now().Unix()
	return true
}

// updateCurrentOffsets updates the current offsets so that a Commit/Heartbeat can pick up the latest
// offsets
func (c *claim) updateCurrentOffsets() {
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
	for _, offset := range offsets {
		if !c.tracking[offset] {
			break
		}
		// Remember current is always "last processed + 1", see the docs on
		// PartitionOffset for a reminder.
		c.offsets.Current = offset + 1
		delete(c.tracking, offset)
	}

	// If we end up with more than 10000 outstanding messages, then something is
	// probably broken in the implementation... since that will cause us to grow
	// forever in memory, let's alert the user
	if len(c.tracking) > 10000 {
		log.Errorf("[%s:%d] has %d uncommitted offsets. You must call Commit.",
			c.topic, c.partID, len(c.tracking))
	}
}

// healthCheck performs a single health check against the claim. If we have failed
// too many times, this will also start a partition release. Returns true if the
// partition is healthy, else false.
func (c *claim) healthCheck() bool {
	// Unclaimed partitions aren't healthy.
	if atomic.LoadInt32(c.claimed) != 1 {
		return false
	}

	// Get velocities; these functions both use the locks so we have to do this before
	// we personally take the lock (to avoid deadlock)
	consumerVelocity := c.ConsumerVelocity()
	partitionVelocity := c.PartitionVelocity()

	c.lock.Lock()
	defer c.lock.Unlock()

	// If our heartbeat is expired, we are definitely unhealthy... don't even bother
	// with checking velocity
	if c.lastHeartbeat < time.Now().Unix()-HeartbeatInterval {
		log.Warningf("[%s:%d] consumer unhealthy by heartbeat test, releasing",
			c.topic, c.partID)
		go c.Release()
		return false
	}

	// In topic claim mode we don't do any velocity checking. It's up to the consumer
	// to ensure they're claiming. TODO: Unclear if this is correct or not.
	if c.options.ClaimEntireTopic {
		return true
	}

	// If current has gone forward of the latest (which is possible, but unlikely)
	// then we are by definition caught up
	if c.offsets.Current >= c.offsets.Latest {
		c.cyclesBehind = 0
		return true
	}

	// If velocity is good, reset cycles behind and exit
	if partitionVelocity <= consumerVelocity {
		if c.cyclesBehind != 0 {
			log.Warningf("[%s:%d] catching up: (resetting warning) CV %0.2f < PV %0.2f (warning #%d)",
				c.topic, c.partID, consumerVelocity, partitionVelocity, c.cyclesBehind)
		}
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
		log.Warningf("[%s:%d] consumer unhealthy, releasing",
			c.topic, c.partID)
		go c.Release()
		return false
	}

	// Clearly we haven't been behind for long enough, so we're still "healthy"
	log.Warningf("[%s:%d] consumer unhealthy: CV %0.2f < PV %0.2f (warning #%d)",
		c.topic, c.partID, consumerVelocity, partitionVelocity, c.cyclesBehind)
	return true
}

// healthCheckLoop runs regularly and will perform a health check. Exits when we are no longer
// a claimed partition
func (c *claim) healthCheckLoop() {
	time.Sleep(<-c.marshal.jitters)
	for c.Claimed() {
		if c.healthCheck() {
			go c.heartbeat()
		}
		time.Sleep(<-c.marshal.jitters)
	}
	log.Debugf("[%s:%d] health check loop exiting", c.topic, c.partID)
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
	// Slow, hits Kafka. Run in a goroutine.
	offsets, err := c.marshal.GetPartitionOffsets(c.topic, c.partID)
	if err != nil {
		log.Errorf("[%s:%d] failed to get offsets: %s", c.topic, c.partID, err)
		return err
	}

	c.updateCurrentOffsets()

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

	state := "----"
	if c.Claimed() {
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
