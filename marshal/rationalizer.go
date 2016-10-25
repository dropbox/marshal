/*
 * portal - marshal
 *
 * a library that implements an algorithm for doing consumer coordination within Kafka, rather
 * than using Zookeeper or another external system.
 *
 */

package marshal

import (
	"sync/atomic"
	"time"

	"github.com/dropbox/kafka"
	"github.com/jpillora/backoff"
)

// kafkaConsumerChannel creates a consumer that continuously attempts to consume messages from
// Kafka for the given partition.
func (c *KafkaCluster) kafkaConsumerChannel(partID int) <-chan message {
	log.Debugf("[%s] rationalize[%d]: starting", c.name, partID)
	out := make(chan message, 1000)
	go c.consumeFromKafka(partID, out, false)
	return out
}

// consumeFromKafka will start consuming messages from Kafka and writing them to the given
// channel forever. It is important that this method closes the "out" channel when it's done,
// as that instructs the downstream goroutine to exit.
func (c *KafkaCluster) consumeFromKafka(partID int, out chan message, startOldest bool) {
	var err error
	var alive bool
	var offsetFirst, offsetNext int64

	// Exit logic -- make sure downstream knows we exited.
	defer func() {
		log.Debugf("[%s] rationalize[%d]: terminating.", c.name, partID)
		close(out)
	}()

	// Try to connect to Kafka. This might sleep a bit and retry since the broker could
	// be down a bit.
	retry := &backoff.Backoff{Min: 500 * time.Millisecond, Jitter: true}
	for ; true; time.Sleep(retry.Duration()) {
		// Figure out how many messages are in this topic. This can fail if the broker handling
		// this partition is down, so we will loop.
		offsetFirst, err = c.broker.OffsetEarliest(MarshalTopic, int32(partID))
		if err != nil {
			log.Errorf("[%s] rationalize[%d]: failed to get offset: %s", c.name, partID, err)
			continue
		}
		offsetNext, err = c.broker.OffsetLatest(MarshalTopic, int32(partID))
		if err != nil {
			log.Errorf("[%s] rationalize[%d]: failed to get offset: %s", c.name, partID, err)
			continue
		}
		log.Debugf("[%s] rationalize[%d]: offsets %d to %d",
			c.name, partID, offsetFirst, offsetNext)

		// TODO: Is there a case where the latest offset is X>0 but there is no data in
		// the partition? does the offset reset to 0?
		if offsetNext == 0 || offsetFirst == offsetNext {
			alive = true
			c.rationalizers.Done()
		}
		break
	}
	retry.Reset()

	// Assume we're starting at the oldest offset for consumption
	consumerConf := kafka.NewConsumerConf(MarshalTopic, int32(partID))
	consumerConf.RetryErrLimit = 1 // Do not retry
	consumerConf.StartOffset = kafka.StartOffsetOldest
	consumerConf.RequestTimeout = c.options.MarshalRequestTimeout

	// Get the offsets of this partition, we're going to arbitrarily pick something that
	// is ~100,000 from the end if there's more than that. This is only if startOldest is
	// false, i.e., we didn't run into a "message too new" situation.
	checkMessageTs := false
	if !startOldest && offsetNext-offsetFirst > 100000 {
		checkMessageTs = true
		consumerConf.StartOffset = offsetNext - 100000
		log.Infof("[%s] rationalize[%d]: fast forwarding to offset %d.",
			c.name, partID, consumerConf.StartOffset)
	}

	consumer, err := c.broker.Consumer(consumerConf)
	if err != nil {
		// Unfortunately this is a termination error, as without being able to consume this
		// partition we can't effectively rationalize.
		log.Errorf("[%s] rationalize[%d]: Failed to create consumer: %s", c.name, partID, err)
		c.Terminate()
		return
	}

	// Consume messages forever, or until told to quit.
	for !c.Terminated() {
		msgb, err := consumer.Consume()
		if err != nil {
			// The internal consumer will do a number of retries. If we get an error here,
			// we're probably in the middle of a partition handoff. We should pause so we
			// don't hammer the cluster, but otherwise continue.
			log.Warningf("[%s] rationalize[%d]: failed to consume: %s", c.name, partID, err)
			time.Sleep(retry.Duration())
			continue
		}
		retry.Reset()

		msg, err := decode(msgb.Value)
		if err != nil {
			// Invalid message in the streac. This should never happen, but if it does, just
			// continue on.
			// TODO: We should probably think about this. If we end up in a situation where
			// one version of this software has a bug that writes invalid messages, it could
			// be doing things we don't anticipate. Of course, crashing all consumers
			// reading that partition is also bad.
			log.Errorf("[%s] rationalize[%d]: %s", c.name, partID, err)

			// In the case where the first message is an invalid message, we need to
			// to notify that we're alive now
			if !alive {
				alive = true
				c.rationalizers.Done()
			}
			continue
		}

		// If we are on our first message, and we started at a non-zero offset, we need
		// to check to make sure that the timestamp is older than a given threshold. If it's
		// too new, that indicates our 100000 try didn't work, so let's go from the start.
		// TODO: This could be a binary search or something.
		if checkMessageTs {
			if int64(msg.Timestamp()) > time.Now().Unix()-HeartbeatInterval*2 {
				log.Warningf("[%s] rationalize[%d]: rewinding, fast-forwarded message was too new",
					c.name, partID)
				go c.consumeFromKafka(partID, out, true)
				return // terminate self.
			}
			checkMessageTs = false
		}

		log.Debugf("[%s] rationalize[%d]: @%d: [%s]", c.name, partID, msgb.Offset, msg.Encode())
		out <- msg

		// This is a one-time thing that fires the first time the rationalizer comes up
		// and makes sure we actually process all of the messages.
		if !alive && msgb.Offset >= offsetNext-1 {
			for len(out) > 0 {
				time.Sleep(100 * time.Millisecond)
			}
			log.Infof("[%s] rationalize[%d]: reached offset %d, now alive",
				c.name, partID, msgb.Offset)
			alive = true
			c.rationalizers.Done()
		}
	}
}

// updateClaim is called whenever we need to adjust a claim structure.
func (c *KafkaCluster) updateClaim(msg *msgHeartbeat) {
	topic := c.getPartitionState(msg.GroupID, msg.Topic, msg.PartID)

	topic.lock.Lock()
	defer topic.lock.Unlock()

	// Note that a heartbeat will just set the claim structure. It's not valid to heartbeat
	// for something you don't own (which is why we have ClaimPartition as a separate
	// message), so we can only assume it's valid.
	topic.partitions[msg.PartID].InstanceID = msg.InstanceID
	topic.partitions[msg.PartID].ClientID = msg.ClientID
	topic.partitions[msg.PartID].GroupID = msg.GroupID
	topic.partitions[msg.PartID].CurrentOffset = msg.CurrentOffset
	topic.partitions[msg.PartID].LastHeartbeat = int64(msg.Time)
	topic.partitions[msg.PartID].LastRelease = 0
}

// releaseClaim is called whenever someone has released their claim on a partition.
func (c *KafkaCluster) releaseClaim(msg *msgReleasingPartition) {
	topic := c.getPartitionState(msg.GroupID, msg.Topic, msg.PartID)

	topic.lock.Lock()
	defer topic.lock.Unlock()

	// The partition must be claimed by the person releasing it
	if !topic.partitions[msg.PartID].checkOwnership(msg, true) {
		log.Warningf(
			"[%s] ReleasePartition %s:%d from client %s that doesn't own it. Dropping.",
			c.name, msg.Topic, msg.PartID, msg.ClientID)
		return
	}

	// Record the offset they told us they last processed, and then set the heartbeat to 0
	// which means this is no longer claimed
	topic.partitions[msg.PartID].CurrentOffset = msg.CurrentOffset
	topic.partitions[msg.PartID].LastHeartbeat = 0
	topic.partitions[msg.PartID].LastRelease = int64(msg.Time)
}

// handleClaim is called whenever we see a ClaimPartition message.
func (c *KafkaCluster) handleClaim(msg *msgClaimingPartition) {
	topic := c.getPartitionState(msg.GroupID, msg.Topic, msg.PartID)

	topic.lock.Lock()
	defer topic.lock.Unlock()

	// Send message to all pending consumers then clear the list (it is a violation of the
	// protocol to send two responses). This fires at the end when we exit so that anybody
	// who is waiting on this partition will know the state has changed.
	defer func() {
		for _, out := range topic.partitions[msg.PartID].pendingClaims {
			close(out)
		}
		topic.partitions[msg.PartID].pendingClaims = nil
	}()

	// If the partition is already claimed, there's nothing we need to do.
	if topic.partitions[msg.PartID].claimed(c.ts) {
		return
	}

	// At this point, the partition is unclaimed, which means we know we have the first
	// ClaimPartition message. As soon as we get it, we fill in the structure which makes
	// us think it's claimed (it is).
	topic.partitions[msg.PartID].InstanceID = msg.InstanceID
	topic.partitions[msg.PartID].ClientID = msg.ClientID
	topic.partitions[msg.PartID].GroupID = msg.GroupID
	topic.partitions[msg.PartID].CurrentOffset = 0 // not present in this message, reset.
	topic.partitions[msg.PartID].LastHeartbeat = int64(msg.Time)
	topic.partitions[msg.PartID].LastRelease = 0
}

// releaseGroup instructs marshallers controlling consumers with a specific groupID to
// pause that consumer group.
func (c *KafkaCluster) releaseGroup(msg *msgReleaseGroup) {
	expiry := time.Unix(int64(msg.MsgExpireTime), 0)
	c.pauseConsumerGroup(msg.GroupID, msg.ClientID, expiry)
}

// rationalize is a goroutine that constantly consumes from a given partition of the marshal
// topic and makes changes to the world state whenever something happens.
func (c *KafkaCluster) rationalize(partID int, in <-chan message) { // Might be in over my head.
	for !c.Terminated() {
		msg, ok := <-in
		if !ok {
			log.Infof("[%s] rationalize[%d]: exiting, channel closed", c.name, partID)
			return
		}

		switch msg.Type() {
		case msgTypeHeartbeat:
			c.updateClaim(msg.(*msgHeartbeat))
		case msgTypeClaimingPartition:
			c.handleClaim(msg.(*msgClaimingPartition))
		case msgTypeReleasingPartition:
			c.releaseClaim(msg.(*msgReleasingPartition))
		case msgTypeClaimingMessages:
			// TODO: Implement.
		case msgTypeReleaseGroup:
			c.releaseGroup(msg.(*msgReleaseGroup))
		}

		// Update step counter so the test suite can wait for messages to be
		// processed in a predictable way (rather than waiting random times)
		atomic.AddInt32(c.rsteps, 1)
	}
	log.Infof("[%s] rationalize[%d]: exiting, Marshaler terminated", c.name, partID)
}
