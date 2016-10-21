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
	"sync"
	"sync/atomic"
	"time"

	"github.com/dropbox/kafka/proto"
)

// Admins will wait a total of consumerReleaseClaimWaitTime
// for paused consumers to release their claims on partitions.
const (
	consumerReleaseClaimWaitSleep = time.Duration(5 * time.Second)
	consumerReleaseClaimWaitTime  = 15 * time.Minute
)

// Admin is used to pause a consumer group and set what position it reads from
// for certain partitions.
type Admin interface {
	// SetConsumerGroupPosition resets consumers to read starting from
	// offsets on each topic, partition pair in positions.
	SetConsumerGroupPosition(groupID string, offsets map[string]map[int]int64) error
}

type consumerGroupAdmin struct {
	clientID     string
	groupID      string
	marshaler    *Marshaler
	pauseTimeout time.Duration

	// claimHealth is 0 if any of our successfully-claimed claims fail to heartbeat.
	claimHealth *int32
	// The lock protects the structs below.
	lock *sync.RWMutex
	// claims are partitions that we've successfully claimed after they've been released,
	// that we'd like to reset the offsets for.
	claims []claimAttempt
	// releaseGroupPartitions keeps track of which Marshal partitions
	// we've produced ReleaseGroup  messages to.
	releaseGroupPartitions []int32
}

// claimAttempt represents a topic, partition we'd like to reset the offset of.
type claimAttempt struct {
	topic  string
	partID int
	// What we'd like to set the offset of a particular partition to be.
	newOffset int64
	// What the current offset of a particular partition is.
	currentOffset int64
}

// addClaimAttempt adds a successfully-claimed partition to our Admin.
func (a *consumerGroupAdmin) addClaimAttempt(topic string,
	partID int, currentOffset, newOffset int64) {

	a.lock.Lock()
	defer a.lock.Unlock()

	c := claimAttempt{
		topic:         topic,
		partID:        partID,
		newOffset:     newOffset,
		currentOffset: currentOffset}
	a.claims = append(a.claims, c)
}

// claimHealth returns whether or not any of the admin's claims have failed to heartbeat.
func (a *consumerGroupAdmin) claimsHealthy() bool {
	return atomic.LoadInt32(a.claimHealth) == 0
}

// NewAdmin returns a new Admin struct bound to a Marshaler. The Marshaler should not have
// any consumers associated with it.
func (m *Marshaler) NewAdmin(groupID string, pauseTimeout time.Duration) (Admin, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if len(m.consumers) != 0 {
		return nil, fmt.Errorf(
			"The Marshaler instance bound to an Admin should not have any consumers.")
	}
	return &consumerGroupAdmin{
		clientID:     m.clientID,
		groupID:      groupID,
		marshaler:    m,
		pauseTimeout: pauseTimeout,

		claimHealth: new(int32),
		lock:        &sync.RWMutex{},
	}, nil
}

// release releases an Admin's claim on a partition. Optionally resets the offset on the partition.
func (a consumerGroupAdmin) release(topic string, partID int, offset int64) bool {
	if err := a.marshaler.ReleasePartition(topic, partID, offset); err != nil {
		log.Errorf("[%s:%d] Admin failed to release partition with offset %d: %s",
			topic, partID, offset, err)
		return false
	}

	return true
}

// releaseClaims releases all claims the Admin has, optionally resetting their offsets.
func (a *consumerGroupAdmin) releaseClaims(resetOffset bool) error {
	a.lock.RLock()
	defer a.lock.RUnlock()

	if !resetOffset {
		log.Infof("Admin releasing claims without resetting offsets.")
	}

	fail := make(chan bool)
	defer close(fail)
	var wg sync.WaitGroup
	wg.Add(len(a.claims))
	for _, claim := range a.claims {
		releaseOffset := claim.currentOffset
		if resetOffset {
			releaseOffset = claim.newOffset
		}

		go func(t string, p int, offset int64) {
			if ok := a.release(t, p, offset); !ok {
				fail <- true
			}
			wg.Done()
		}(claim.topic, claim.partID, releaseOffset)
	}

	// Wait on all workers to reset their respective Kafka offset.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-fail:
		return fmt.Errorf("Admin failed to reset Kafka offset!")
	case <-done:
		return nil
	}
}

// Send a single heartbeat using the given offset.
func (a *consumerGroupAdmin) heartbeat(
	topic string, partID int, offset int64) (ok bool) {
	// If we fail to heartbeat, record this in claimHealth.
	// The Admin will take care of cleaning up other claims.
	if err := a.marshaler.Heartbeat(topic, partID, offset); err != nil {
		log.Errorf("[%s:%d] Admin failed to heartbeat. It is now unhealthy "+
			"and will not reset offsets.", topic, partID)
		atomic.StoreInt32(a.claimHealth, 0)
		return false
	}
	return true
}

// heartbeatLoop heartbeats as if we had a claim to this partition and were simply
// not reading past where the previous owner had left off.
//
// Callers can signal for the heartbeatLoop to stop by sending a message on the
// stopHeartbeat channel; the heartbeatLoop will close the heartbeatStopped
// channel after it has in fact stopped.
func (a *consumerGroupAdmin) heartbeatLoop(
	topic string, partID int, lastOffset int64,
	stopHeartbeat, heartbeatStopped chan struct{}) {

	defer close(heartbeatStopped)
	// We send the first heartbeat right away (instead of waiting for an entire
	// heartbeat interval to pass).
	if ok := a.heartbeat(topic, partID, lastOffset); !ok {
		return
	}
	for {
		select {
		// Stop claimHealth either when all topic, partitions have been successfully claimed,
		// or the Admin has failed to do so and needs to abort.
		case <-stopHeartbeat:
			return
		// Note that waiting for the heartbeat interval in a select statement (instead of
		// using time.Sleep) allows the heartbeat to stop right away.
		case <-time.After(<-a.marshaler.cluster.jitters):
			if ok := a.heartbeat(topic, partID, lastOffset); !ok {
				return
			}
		}
	}
}

// claimAndHeartbeat attempts to claim a partition released by a paused consumer.
// It heartbeats the previous offset.
func (a *consumerGroupAdmin) claimAndHeartbeat(topic string,
	partID int, newOffset int64,
	stopHeartbeat, heartbeatStopped chan struct{}) bool {

	// Get current offsets, which we will try to keep claimHealth.
	partitionClaim := a.marshaler.GetLastPartitionClaim(topic, partID)

	// Next, try to claim the partition.
	if !a.marshaler.ClaimPartition(topic, partID) {
		log.Errorf("[%s:%d] Admin couldn't claim partition to set Kafka offset",
			topic, partID)
		return false
	}

	// Continuously heartbeat the last offsets.
	a.addClaimAttempt(topic, partID, partitionClaim.CurrentOffset, newOffset)
	go a.heartbeatLoop(
		topic, partID, partitionClaim.CurrentOffset, stopHeartbeat, heartbeatStopped)
	return true
}

// constructReleaseGroupMessage returns a ReleaseGroup message to write to the Marshal topic.
func (a *consumerGroupAdmin) constructReleaseGroupMessage() *msgReleaseGroup {
	now := time.Now()
	base := &msgBase{
		Time:       int(now.Unix()),
		InstanceID: a.marshaler.instanceID,
		ClientID:   a.clientID,
		GroupID:    a.groupID,
	}
	return &msgReleaseGroup{
		msgBase:       *base,
		MsgExpireTime: int(now.Add(a.pauseTimeout).Unix()),
	}
}

// sendReleaseGroupMessage sends a ReleaseGroup message for a consumer group reading froma given topic.
func (a *consumerGroupAdmin) sendReleaseGroupMessage(topicName string, partID int) error {
	a.lock.Lock()
	defer a.lock.Unlock()
	topic := a.marshaler.cluster.getPartitionState(a.groupID, topicName, partID)

	for _, partition := range a.releaseGroupPartitions {
		if int32(topic.claimPartition) == partition {
			return nil
		}
	}

	a.releaseGroupPartitions = append(a.releaseGroupPartitions, int32(topic.claimPartition))
	rg := a.constructReleaseGroupMessage()
	_, err := a.marshaler.cluster.producer.Produce(MarshalTopic,
		int32(topic.claimPartition), &proto.Message{Value: []byte(rg.Encode())})
	return err
}

// pauseGroupAndWaitForRelease is called for every partition we'd like to change the offset for.
// It first sends a ReleaseGroup message to Marshal, then waits for it to be released,
// then attempts to claim it.
func (a *consumerGroupAdmin) pauseGroupAndWaitForRelease(topicName string, partID int) bool {
	if err := a.sendReleaseGroupMessage(topicName, partID); err != nil {
		log.Errorf("[%s:%d] Admin failed to produce ReleaseMessage group to Kafka: %s",
			topicName, partID, err)
		return false
	}

	// Wait for the paused consumer to release its claim.
	tick := time.NewTicker(consumerReleaseClaimWaitSleep)
	defer tick.Stop()

	select {
	case <-tick.C:
		if cl := a.marshaler.GetPartitionClaim(topicName, partID); cl.LastHeartbeat == 0 {
			break
		}
	case <-time.After(consumerReleaseClaimWaitTime):
		return false
	}
	return true
}

// SetConsumerGroupPosition sets where the consumer group identified by groupID
// should start reading from for given partitions.
func (a *consumerGroupAdmin) SetConsumerGroupPosition(groupID string,
	offsets map[string]map[int]int64) error {

	log.Infof("Admin %s going to pause consumer group %s", a.clientID, groupID)
	var wg sync.WaitGroup
	// Send out a ReleaseGroup message to Marshal for each partition we want to set the position for,
	// then wait for all the partitions to be released.
	fail := make(chan bool)
	defer close(fail)
	for topicName, partitionOffsets := range offsets {
		for partID := range partitionOffsets {
			wg.Add(1)
			go func(topicName string, partID int) {
				if ok := a.pauseGroupAndWaitForRelease(topicName, partID); !ok {
					fail <- true
				}
				wg.Done()
			}(topicName, partID)
		}
	}

	// Wait on all partitions to be released, or one failure.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-fail:
		return fmt.Errorf("Consumer group %s has not been reset", groupID)
	case <-done:
		break
	}

	// Attempt to claim the now-released partitions, then heartbeat old offsets after a successful claim.
	log.Infof("Admin now claiming released partitions.")
	claimFailures := make(chan bool)
	defer close(claimFailures)

	var claimsWg sync.WaitGroup
	// Sending a message on stopHeartbeat instructs all successfully claimed and
	// heartbeating claims to stop.  heartbeatStopped will be closed after all
	// heartbeating has in fact stopped.
	stopHeartbeat := make(chan struct{})
	heartbeatStopped := make(chan struct{})
	for topicName, partitionOffsets := range offsets {
		for partID, offset := range partitionOffsets {
			claimsWg.Add(1)

			go func(topicName string, partID int, offset int64) {
				ok := a.claimAndHeartbeat(
					topicName, partID, offset, stopHeartbeat, heartbeatStopped)
				if !ok {
					claimFailures <- true
				}
				claimsWg.Done()
			}(topicName, partID, offset)
		}
	}

	// Wait on attempts to claim partitions.
	claimsDone := make(chan struct{})
	go func() {
		claimsWg.Wait()
		close(claimsDone)
	}()

	select {
	case <-claimFailures:
		err := errors.New("Couldn't claim a partition -- admin failed to reset consumer group position! " +
			"Now releasing all existing claims without resetting offsets.")
		close(stopHeartbeat)
		<-heartbeatStopped
		a.releaseClaims(false)
		return err
	case <-claimsDone:
		close(stopHeartbeat)
		// It's critical that we don't perform an offset-resetting release operation until
		// after heartbeating has stopped.  Otherwise, a subsequent heartbeat could undo
		// the offset reset (the heartbeats use the previous offsets).
		<-heartbeatStopped
		// Release claims and reset offsets, if all claims have been successfully heartbeating.
		// If not, we'll release claims and not reset offsets.
		return a.releaseClaims(a.claimsHealthy())
	}
}
