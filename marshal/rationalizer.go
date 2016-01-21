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

	"github.com/jpillora/backoff"
	"github.com/optiopay/kafka"
)

// kafkaConsumerChannel creates a consumer that continuously attempts to consume messages from
// Kafka for the given partition.
func (m *Marshaler) kafkaConsumerChannel(partID int) <-chan message {
	log.Debugf("rationalize[%d]: starting", partID)
	out := make(chan message, 1000)
	go m.consumeFromKafka(partID, out, false)
	return out
}

// consumeFromKafka will start consuming messages from Kafka and writing them to the given
// channel forever. It is important that this method closes the "out" channel when it's done,
// as that instructs the downstream goroutine to exit.
func (m *Marshaler) consumeFromKafka(partID int, out chan message, startOldest bool) {
	var err error
	var alive bool
	var offsetFirst, offsetNext int64

	// Try to connect to Kafka. This might sleep a bit and retry since the broker could
	// be down a bit.
	retry := &backoff.Backoff{Min: 500 * time.Millisecond, Jitter: true}
	for ; true; time.Sleep(retry.Duration()) {
		// Figure out how many messages are in this topic. This can fail if the broker handling
		// this partition is down, so we will loop.
		offsetFirst, err = m.kafka.OffsetEarliest(MarshalTopic, int32(partID))
		if err != nil {
			log.Errorf("rationalize[%d]: failed to get offset: %s", partID, err)
			continue
		}
		offsetNext, err = m.kafka.OffsetLatest(MarshalTopic, int32(partID))
		if err != nil {
			log.Errorf("rationalize[%d]: failed to get offset: %s", partID, err)
			continue
		}
		log.Debugf("rationalize[%d]: offsets %d to %d", partID, offsetFirst, offsetNext)

		// TODO: Is there a case where the latest offset is X>0 but there is no data in
		// the partition? does the offset reset to 0?
		if offsetNext == 0 || offsetFirst == offsetNext {
			alive = true
			m.rationalizers.Done()
		}
		break
	}
	retry.Reset()

	// Assume we're starting at the oldest offset for consumption
	consumerConf := kafka.NewConsumerConf(MarshalTopic, int32(partID))
	consumerConf.StartOffset = kafka.StartOffsetOldest
	consumerConf.RequestTimeout = 1 * time.Second
	consumerConf.Logger = &optiopayLoggerShim{l: log}

	// Get the offsets of this partition, we're going to arbitrarily pick something that
	// is ~100,000 from the end if there's more than that. This is only if startOldest is
	// false, i.e., we didn't run into a "message too new" situation.
	checkMessageTs := false
	if !startOldest && offsetNext-offsetFirst > 100000 {
		checkMessageTs = true
		consumerConf.StartOffset = offsetNext - 100000
		log.Infof("rationalize[%d]: fast forwarding to offset %d.",
			partID, consumerConf.StartOffset)
	}

	consumer, err := m.kafka.Consumer(consumerConf)
	if err != nil {
		// Unfortunately this is a fatal error, as without being able to consume this partition
		// we can't effectively rationalize.
		log.Fatalf("rationalize[%d]: Failed to create consumer: %s", partID, err)
	}

	// Consume messages forever, or until told to quit.
	for !m.Terminated() {
		msgb, err := consumer.Consume()
		if err != nil {
			// The internal consumer will do a number of retries. If we get an error here,
			// we're probably in the middle of a partition handoff. We should pause so we
			// don't hammer the cluster, but otherwise continue.
			log.Warningf("rationalize[%d]: failed to consume: %s", partID, err)
			time.Sleep(retry.Duration())
			continue
		}
		retry.Reset()

		msg, err := decode(msgb.Value)
		if err != nil {
			// Invalid message in the stream. This should never happen, but if it does, just
			// continue on.
			// TODO: We should probably think about this. If we end up in a situation where
			// one version of this software has a bug that writes invalid messages, it could
			// be doing things we don't anticipate. Of course, crashing all consumers
			// reading that partition is also bad.
			log.Errorf("rationalize[%d]: %s", partID, err)

			// In the case where the first message is an invalid message, we need to
			// to notify that we're alive now
			if !alive {
				alive = true
				m.rationalizers.Done()
			}
			continue
		}

		// If we are on our first message, and we started at a non-zero offset, we need
		// to check to make sure that the timestamp is older than a given threshold. If it's
		// too new, that indicates our 100000 try didn't work, so let's go from the start.
		// TODO: This could be a binary search or something.
		if checkMessageTs {
			if int64(msg.Timestamp()) > time.Now().Unix()-HeartbeatInterval*2 {
				log.Warningf("rationalize[%d]: rewinding, fast-forwarded message was too new",
					partID)
				go m.consumeFromKafka(partID, out, true)
				return // terminate self.
			}
			checkMessageTs = false
		}

		log.Debugf("rationalize[%d]: @%d: [%s]", partID, msgb.Offset, msg.Encode())
		out <- msg

		// This is a one-time thing that fires the first time the rationalizer comes up
		// and makes sure we actually process all of the messages.
		if !alive && msgb.Offset >= offsetNext-1 {
			for len(out) > 0 {
				time.Sleep(100 * time.Millisecond)
			}
			log.Infof("rationalize[%d]: reached offset %d, now alive",
				partID, msgb.Offset)
			alive = true
			m.rationalizers.Done()
		}
	}

	// Inform and close the channel so our downstream goroutine also exits.
	log.Debugf("rationalize[%d]: terminating.", partID)
	close(out)
}

// updateClaim is called whenever we need to adjust a claim structure.
func (m *Marshaler) updateClaim(msg *msgHeartbeat) {
	topic := m.getPartitionState(msg.GroupID, msg.Topic, msg.PartID)

	topic.lock.Lock()
	defer topic.lock.Unlock()

	// Note that a heartbeat will just set the claim structure. It's not valid to heartbeat
	// for something you don't own (which is why we have ClaimPartition as a separate
	// message), so we can only assume it's valid.
	topic.partitions[msg.PartID].ClientID = msg.ClientID
	topic.partitions[msg.PartID].GroupID = msg.GroupID
	topic.partitions[msg.PartID].LastOffset = msg.LastOffset
	topic.partitions[msg.PartID].LastHeartbeat = int64(msg.Time)
}

// releaseClaim is called whenever someone has released their claim on a partition.
func (m *Marshaler) releaseClaim(msg *msgReleasingPartition) {
	topic := m.getPartitionState(msg.GroupID, msg.Topic, msg.PartID)

	topic.lock.Lock()
	defer topic.lock.Unlock()

	// The partition must be claimed by the person releasing it
	if topic.partitions[msg.PartID].ClientID != msg.ClientID ||
		topic.partitions[msg.PartID].GroupID != msg.GroupID {
		log.Warningf("ReleasePartition message from client that doesn't own it. Dropping.")
		return
	}

	// Record the offset they told us they last processed, and then set the heartbeat to 0
	// which means this is no longer claimed
	topic.partitions[msg.PartID].LastOffset = msg.LastOffset
	topic.partitions[msg.PartID].LastHeartbeat = 0
}

// handleClaim is called whenever we see a ClaimPartition message.
func (m *Marshaler) handleClaim(msg *msgClaimingPartition) {
	topic := m.getPartitionState(msg.GroupID, msg.Topic, msg.PartID)

	topic.lock.Lock()
	defer topic.lock.Unlock()

	// Send message to all pending consumers then clear the list (it is a violation of the
	// protocol to send two responses)
	fireEvents := func(evt bool) {
		for _, out := range topic.partitions[msg.PartID].pendingClaims {
			out <- evt
		}
		topic.partitions[msg.PartID].pendingClaims = nil
	}

	// Claim logic: if the claim is for an already claimed partition, we can end now and decide
	// whether or not to fire.
	if topic.partitions[msg.PartID].isClaimed(m.ts) {
		// The ClaimPartition message needs to be from us, or we should just return
		if msg.ClientID == m.clientID && msg.GroupID == m.groupID {
			// Now determine if we own the partition claim and let us know whether we do
			// or not
			if topic.partitions[msg.PartID].ClientID == m.clientID &&
				topic.partitions[msg.PartID].GroupID == m.groupID {
				fireEvents(true)
			} else {
				fireEvents(false)
			}
		}
		return
	}

	// At this point, the partition is unclaimed, which means we know we have the first
	// ClaimPartition message. As soon as we get it, we fill in the structure which makes
	// us think it's claimed (it is).
	topic.partitions[msg.PartID].ClientID = msg.ClientID
	topic.partitions[msg.PartID].GroupID = msg.GroupID
	topic.partitions[msg.PartID].LastOffset = 0 // not present in this message, reset.
	topic.partitions[msg.PartID].LastHeartbeat = int64(msg.Time)

	// Now we can advise that this partition has been handled
	if msg.ClientID == m.clientID && msg.GroupID == m.groupID {
		fireEvents(true)
	} else {
		fireEvents(false)
	}
}

// rationalize is a goroutine that constantly consumes from a given partition of the marshal
// topic and makes changes to the world state whenever something happens.
func (m *Marshaler) rationalize(partID int, in <-chan message) { // Might be in over my head.
	for !m.Terminated() {
		msg, ok := <-in
		if !ok {
			log.Infof("rationalize[%d]: exiting, channel closed", partID)
			return
		}

		switch msg.Type() {
		case msgTypeHeartbeat:
			m.updateClaim(msg.(*msgHeartbeat))
		case msgTypeClaimingPartition:
			m.handleClaim(msg.(*msgClaimingPartition))
		case msgTypeReleasingPartition:
			m.releaseClaim(msg.(*msgReleasingPartition))
		case msgTypeClaimingMessages:
			// TODO: Implement.
		}

		// Update step counter so the test suite can wait for messages to be
		// processed in a predictable way (rather than waiting random times)
		atomic.AddInt32(m.rsteps, 1)
	}
	log.Infof("rationalize[%d]: exiting, Marshaler terminated", partID)
}
