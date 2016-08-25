package marshal

import (
	"sync"
	"time"

	. "gopkg.in/check.v1"

	"github.com/dropbox/kafka/kafkatest"
	"github.com/dropbox/kafka/proto"
)

var _ = Suite(&ClaimSuite{})

type ClaimSuite struct {
	c  *C
	s  *kafkatest.Server
	m  *Marshaler
	ch chan *Message
	cl *claim
}

func (s *ClaimSuite) SetUpTest(c *C) {
	s.c = c
	s.s = StartServer()
	s.ch = make(chan *Message, 10)
	var err error
	s.m, err = NewMarshaler("cl", "gr", []string{s.s.Addr()})
	c.Assert(err, IsNil)
	s.cl = newClaim("test16", 0, s.m, nil, s.ch, NewConsumerOptions())
	c.Assert(s.cl, NotNil)
}

func (s *ClaimSuite) TearDownTest(c *C) {
	if s.cl != nil {
		s.cl.Release()
	}
	if s.m != nil {
		s.m.Terminate()
	}
	if s.s != nil {
		s.s.Close()
	}
}

func (s *ClaimSuite) Produce(topicName string, partID int, msgs ...string) int64 {
	var protos []*proto.Message
	for _, msg := range msgs {
		protos = append(protos, &proto.Message{Value: []byte(msg)})
	}
	offset, err := s.m.cluster.producer.Produce(topicName, int32(partID), protos...)
	s.c.Assert(err, IsNil)
	return offset
}

func (s *ClaimSuite) TestOffsetUpdates(c *C) {
	// Test that the updateOffsets function works and updates offsets from Kafka
	c.Assert(s.cl.updateOffsets(), IsNil)
	c.Assert(s.Produce("test16", 0, "m1", "m2", "m3"), Equals, int64(2))
	c.Assert(s.cl.updateOffsets(), IsNil)
	c.Assert(s.cl.offsets.Latest, Equals, int64(3))
}

func (s *ClaimSuite) consumeOne(c *C) *Message {
	select {
	case msg := <-s.ch:
		return msg
	case <-time.After(3 * time.Second):
		c.Error("Timed out consuming a message.")
	}
	return nil
}

func (s *ClaimSuite) TestCommit(c *C) {
	// Test the commit message flow, ensuring that our offset only gets updated when
	// we have properly committed messages
	c.Assert(s.Produce("test16", 0, "m1", "m2", "m3", "m4", "m5", "m6"), Equals, int64(5))
	c.Assert(s.cl.updateOffsets(), IsNil)
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.cl.offsets.Current, Equals, int64(0))
	c.Assert(s.cl.offsets.Earliest, Equals, int64(0))
	c.Assert(s.cl.offsets.Latest, Equals, int64(6))

	// Consume 1, heartbeat... offsets still 0
	msg1 := s.consumeOne(c)
	c.Assert(msg1.Value, DeepEquals, []byte("m1"))
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.cl.offsets.Current, Equals, int64(0))
	s.cl.lock.RLock()
	c.Assert(s.cl.tracking[0].committed, Equals, false)
	s.cl.lock.RUnlock()

	// Consume 2, still 0
	msg2 := s.consumeOne(c)
	c.Assert(msg2.Value, DeepEquals, []byte("m2"))
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.cl.offsets.Current, Equals, int64(0))

	// Commit 1, offset 1 but only after heartbeat phase
	c.Assert(s.cl.Commit(msg1.Offset), IsNil)
	c.Assert(s.cl.offsets.Current, Equals, int64(0))
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.cl.offsets.Current, Equals, int64(1))
	c.Assert(s.cl.numTrackingOffsets(), Equals, 5)

	// Consume 3, heartbeat, offset 1
	msg3 := s.consumeOne(c)
	c.Assert(msg3.Value, DeepEquals, []byte("m3"))
	c.Assert(s.cl.offsets.Current, Equals, int64(1))
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.cl.offsets.Current, Equals, int64(1))

	// Commit #3, offset will stay 1!
	c.Assert(s.cl.Commit(msg3.Offset), IsNil)
	c.Assert(s.cl.offsets.Current, Equals, int64(1))
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.cl.offsets.Current, Equals, int64(1))
	c.Assert(s.cl.numTrackingOffsets(), Equals, 5)

	// Commit #2, offset now advances to 3
	c.Assert(s.cl.Commit(msg2.Offset), IsNil)
	c.Assert(s.cl.offsets.Current, Equals, int64(1))
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.cl.offsets.Current, Equals, int64(3))
	c.Assert(s.cl.numTrackingOffsets(), Equals, 3)

	// Attempt to commit invalid offset (never seen), make sure it errors
	msg3.Offset = 95
	c.Assert(s.cl.Commit(msg3.Offset), NotNil)

	// Commit the rest
	c.Assert(s.cl.Commit(s.consumeOne(c).Offset), IsNil)
	c.Assert(s.cl.Commit(s.consumeOne(c).Offset), IsNil)
	c.Assert(s.cl.Commit(s.consumeOne(c).Offset), IsNil)
	c.Assert(s.cl.offsets.Current, Equals, int64(3))
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.cl.offsets.Current, Equals, int64(6))
	c.Assert(s.cl.numTrackingOffsets(), Equals, 0)
}

func (s *ClaimSuite) TestMessageExpiration(c *C) {
	// Setup expiration settings
	expires := make(chan ExpiredMessageToken, 10)
	s.cl.options.ExpiredMessageCallback = func(token ExpiredMessageToken) {
		expires <- token
	}
	s.cl.options.ExpiredMessageTimeout = 100 * time.Millisecond

	// Three messages
	c.Assert(s.Produce("test16", 0, "m1", "m2", "m3"), Equals, int64(2))
	c.Assert(s.cl.updateOffsets(), IsNil)
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.cl.offsets.Current, Equals, int64(0))
	c.Assert(s.cl.offsets.Earliest, Equals, int64(0))
	c.Assert(s.cl.offsets.Latest, Equals, int64(3))

	// Consume 1, heartbeat... offsets still 0
	msg1 := s.consumeOne(c)
	c.Assert(msg1.Value, DeepEquals, []byte("m1"))
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.cl.offsets.Current, Equals, int64(0))
	s.cl.lock.RLock()
	c.Assert(s.cl.tracking[0].committed, Equals, false)
	c.Assert(s.cl.tracking[1].committed, Equals, false)
	c.Assert(s.cl.tracking[2].committed, Equals, false)
	s.cl.lock.RUnlock()

	// Consume 2, heartbeat... offsets still 0
	msg2 := s.consumeOne(c)
	c.Assert(msg2.Value, DeepEquals, []byte("m2"))
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.cl.offsets.Current, Equals, int64(0))
	s.cl.lock.RLock()
	c.Assert(s.cl.tracking[0].committed, Equals, false)
	c.Assert(s.cl.tracking[1].committed, Equals, false)
	c.Assert(s.cl.tracking[2].committed, Equals, false)
	s.cl.lock.RUnlock()

	// Consume 3, heartbeat... offsets still 0
	msg3 := s.consumeOne(c)
	c.Assert(msg3.Value, DeepEquals, []byte("m3"))
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.cl.offsets.Current, Equals, int64(0))
	s.cl.lock.RLock()
	c.Assert(s.cl.tracking[0].committed, Equals, false)
	c.Assert(s.cl.tracking[1].committed, Equals, false)
	c.Assert(s.cl.tracking[2].committed, Equals, false)
	s.cl.lock.RUnlock()

	// Now commit first / third and heartbeat, offset is 1
	c.Assert(s.cl.Commit(msg1.Offset), IsNil)
	c.Assert(s.cl.Commit(msg3.Offset), IsNil)
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.cl.offsets.Current, Equals, int64(msg2.Offset))
	c.Assert(s.cl.numTrackingOffsets(), Equals, 1)
	s.cl.lock.RLock()
	c.Assert(s.cl.tracking[1].committed, Equals, false)
	s.cl.lock.RUnlock()
	c.Assert(len(expires), Equals, 0) // Not guaranteed to be true, but...

	// Now wait and fire a heartbeat, we should get notified
	time.Sleep(101 * time.Millisecond)
	c.Assert(s.cl.heartbeat(), Equals, true)

	select {
	case token := <-expires:
		// First pass, just ask to retry
		c.Assert(token.Retry(), IsNil)
	case <-time.After(200 * time.Millisecond):
		c.Log("Failed to get expiration callback")
		c.FailNow()
	}

	// We should be able to consume the message again now
	msg2_2 := s.consumeOne(c)
	c.Assert(msg2_2.Value, DeepEquals, []byte("m2"))
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.cl.offsets.Current, Equals, int64(1))
	s.cl.lock.RLock()
	c.Assert(s.cl.tracking[1].committed, Equals, false)
	s.cl.lock.RUnlock()
}

func (s *ClaimSuite) waitForTrackingOffsets(c *C, ct int) {
	for i := 0; i < 100; i++ {
		if s.cl.numTrackingOffsets() != ct {
			time.Sleep(10 * time.Millisecond)
			continue
		}
	}
	c.Assert(s.cl.numTrackingOffsets(), Equals, ct)
}

func (s *ClaimSuite) TestFlush(c *C) {
	// Test the commit message flow, ensuring that our offset only gets updated when
	// we have properly committed messages
	//
	// Basically the same as the heartbeat test, since a flush triggers a heartbeat
	c.Assert(s.Produce("test16", 0, "m1", "m2", "m3", "m4", "m5", "m6"), Equals, int64(5))
	c.Assert(s.cl.updateOffsets(), IsNil)
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.cl.offsets.Current, Equals, int64(0))
	c.Assert(s.cl.offsets.Earliest, Equals, int64(0))
	c.Assert(s.cl.offsets.Latest, Equals, int64(6))
	s.waitForTrackingOffsets(c, 6)

	// Consume 1, Flush... offsets still 0
	msg1 := s.consumeOne(c)
	c.Assert(msg1.Value, DeepEquals, []byte("m1"))
	c.Assert(msg1.Offset, Equals, int64(0))
	c.Assert(s.cl.Flush(), IsNil)
	c.Assert(s.cl.offsets.Current, Equals, int64(0))
	s.cl.lock.RLock()
	val, ok := s.cl.tracking[0]
	c.Assert(ok, Equals, true)
	c.Assert(val.committed, Equals, false)
	s.cl.lock.RUnlock()

	// Consume 2, still 0
	msg2 := s.consumeOne(c)
	c.Assert(msg2.Value, DeepEquals, []byte("m2"))
	c.Assert(msg2.Offset, Equals, int64(1))
	c.Assert(s.cl.Flush(), IsNil)
	c.Assert(s.cl.offsets.Current, Equals, int64(0))
	c.Assert(s.cl.numTrackingOffsets(), Equals, 6)

	// Commit 1, offset 1 but only after Flush phase
	c.Assert(s.cl.Commit(msg1.Offset), IsNil)
	c.Assert(s.cl.offsets.Current, Equals, int64(0))
	c.Assert(s.cl.Flush(), IsNil)
	c.Assert(s.cl.offsets.Current, Equals, int64(1))
	c.Assert(s.cl.numTrackingOffsets(), Equals, 5)

	// Produce some more
	c.Assert(s.Produce("test16", 0, "m7"), Equals, int64(6))
	s.waitForTrackingOffsets(c, 6)

	// Consume 3, Flush, offset 1
	msg3 := s.consumeOne(c)
	c.Assert(msg3.Value, DeepEquals, []byte("m3"))
	c.Assert(msg3.Offset, Equals, int64(2))
	c.Assert(s.cl.offsets.Current, Equals, int64(1))
	c.Assert(s.cl.Flush(), IsNil)
	c.Assert(s.cl.offsets.Current, Equals, int64(1))

	// Assert that the above didn't update the Latest offset, the Flush
	// flow doesn't (unlike heartbeat which does)
	c.Assert(s.cl.offsets.Latest, Equals, int64(6))

	// Commit #3, offset will stay 1! we're still tracking 6 because the
	// committed one in middle position must stay tracked until the
	// previous messages are committed
	c.Assert(s.cl.Commit(msg3.Offset), IsNil)
	c.Assert(s.cl.offsets.Current, Equals, int64(1))
	c.Assert(s.cl.Flush(), IsNil)
	c.Assert(s.cl.offsets.Current, Equals, int64(1))
	c.Assert(s.cl.numTrackingOffsets(), Equals, 6)

	// Now a heartbeat happens, it should change nothing except Latest
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.cl.offsets.Current, Equals, int64(1))
	c.Assert(s.cl.numTrackingOffsets(), Equals, 6)
	c.Assert(s.cl.offsets.Latest, Equals, int64(7))

	// Commit #2, offset now advances to 3 and the outstanding is 4
	c.Assert(s.cl.Commit(msg2.Offset), IsNil)
	c.Assert(s.cl.offsets.Current, Equals, int64(1))
	c.Assert(s.cl.Flush(), IsNil)
	c.Assert(s.cl.offsets.Current, Equals, int64(3))
	c.Assert(s.cl.numTrackingOffsets(), Equals, 4)

	// Attempt to commit invalid offset (never seen), make sure it errors
	msg3.Offset = 95
	c.Assert(s.cl.Commit(msg3.Offset), NotNil)

	// Commit the rest
	c.Assert(s.cl.Commit(s.consumeOne(c).Offset), IsNil)
	c.Assert(s.cl.Commit(s.consumeOne(c).Offset), IsNil)
	c.Assert(s.cl.Commit(s.consumeOne(c).Offset), IsNil)
	c.Assert(s.cl.Commit(s.consumeOne(c).Offset), IsNil)
	c.Assert(s.cl.offsets.Current, Equals, int64(3))
	c.Assert(s.cl.Flush(), IsNil)
	c.Assert(s.cl.offsets.Current, Equals, int64(7))
	c.Assert(s.cl.numTrackingOffsets(), Equals, 0)

	// One last heartbeat, should be no change from the above Flush since
	// nothing has happened
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.cl.offsets.Current, Equals, int64(7))
	c.Assert(s.cl.numTrackingOffsets(), Equals, 0)
	c.Assert(s.cl.offsets.Latest, Equals, int64(7))
}

func (s *ClaimSuite) TestOrderedConsume(c *C) {
	// Turn on ordered consumption
	s.cl.options.StrictOrdering = true
	c.Assert(s.Produce("test16", 0, "m1", "m2", "m3", "m4", "m5", "m6"), Equals, int64(5))
	c.Assert(s.cl.updateOffsets(), IsNil)
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.cl.offsets.Current, Equals, int64(0))
	c.Assert(s.cl.offsets.Earliest, Equals, int64(0))
	c.Assert(s.cl.offsets.Latest, Equals, int64(6))

	// Consume 1, heartbeat... offsets still 0
	msg1 := s.consumeOne(c)
	s.cl.lock.RLock()
	c.Assert(s.cl.numTrackingOffsets(), Equals, 1) // Only one, since ordering.
	s.cl.lock.RUnlock()
	c.Assert(msg1.Value, DeepEquals, []byte("m1"))
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.cl.offsets.Current, Equals, int64(0))
	c.Assert(s.cl.tracking[0].committed, Equals, false)

	// Attempt to consume a message, but we expect it to fail (i.e. no message to be
	// available)
	select {
	case <-s.ch:
		c.Error("Expected no message, but message was returned")
	case <-time.After(300 * time.Millisecond):
		// Good.
	}

	// Now commit, and then try to consume again, it should work
	c.Assert(s.cl.Commit(msg1.Offset), IsNil)
	msg2 := s.consumeOne(c)
	c.Assert(msg2.Value, DeepEquals, []byte("m2"))
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.cl.offsets.Current, Equals, int64(1))
}

func (s *ClaimSuite) BenchmarkConsumeAndCommit(c *C) {
	// Produce N messages for consumption into the test partition and hopefully this
	// doesn't end up being the really slow part of the operation
	msgs := make([]string, 0, c.N)
	for i := 0; i < c.N; i++ {
		msgs = append(msgs, "message")
	}
	s.Produce("test16", 0, msgs...)

	// Now consume everything and immediately commit it
	for i := 0; i < c.N; i++ {
		if msg := s.consumeOne(c); msg != nil {
			s.cl.Commit(msg.Offset)
		}
	}
}

func (s *ClaimSuite) BenchmarkOrderedConsumeAndCommit(c *C) {
	// Turn on ordering to slow everything down...
	s.cl.options.StrictOrdering = true

	// Produce N messages for consumption into the test partition and hopefully this
	// doesn't end up being the really slow part of the operation
	msgs := make([]string, 0, c.N)
	for i := 0; i < c.N; i++ {
		msgs = append(msgs, "message")
	}
	s.Produce("test16", 0, msgs...)

	// Now consume everything and immediately commit it
	for i := 0; i < c.N; i++ {
		if msg := s.consumeOne(c); msg != nil {
			s.cl.Commit(msg.Offset)
		}
	}
}

func (s *ClaimSuite) assertRelease(c *C) {
	for i := 0; i < 100; i++ {
		time.Sleep(30 * time.Millisecond)
		if s.cl.Claimed() == false {
			break
		}
	}
	c.Assert(s.cl.Claimed(), Equals, false)
}

func (s *ClaimSuite) TestRelease(c *C) {
	// Test that calling Release on a claim properly releases the partition
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastHeartbeat, Not(Equals), int64(0))
	c.Assert(s.cl.Claimed(), Equals, true)
	c.Assert(s.cl.Release(), Equals, true)
	s.assertRelease(c)
	c.Assert(s.m.cluster.waitForRsteps(3), Equals, 3)
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastHeartbeat, Equals, int64(0))
	c.Assert(s.cl.Release(), Equals, false)
}

func (s *ClaimSuite) TestTerminate(c *C) {
	// Test that calling Release on a claim properly sets the flag and commits offsets
	// for the partition
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastHeartbeat, Not(Equals), int64(0))
	c.Assert(s.cl.Claimed(), Equals, true)
	c.Assert(s.cl.Terminate(), Equals, true)
	c.Assert(s.cl.Claimed(), Equals, true)
}

func (s *ClaimSuite) TestTerminateDoesNotDeadlock(c *C) {
	// Test that termination is not blocked by a full messages channel
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastHeartbeat, Not(Equals), int64(0))
	c.Assert(s.cl.Claimed(), Equals, true)

	// Replace message chan with length 1 chan for testing
	s.cl.lock.Lock()
	s.cl.messages = make(chan *Message, 1)
	s.cl.lock.Unlock()

	// Now produce 2 messages and wait for them to be consumed
	c.Assert(s.Produce("test16", 0, "m1", "m2"), Equals, int64(1))
	s.waitForTrackingOffsets(c, 2)

	// Assert message channel has 1 message in it, we have 2 tracking but 1 message
	// in channel because second is blocked
	c.Assert(len(s.cl.messages), Equals, 1)

	// Assert that messageLock is being held, this works by sending a goroutine to
	// get the lock and then in our current function we sleep a bit. If the sleep expires,
	// that means the goroutine was blocked (or never scheduled). We get around that by
	// using the WaitGroup to make sure it actually scheduled.
	wasScheduled := &sync.WaitGroup{}
	wasScheduled.Add(1)
	probablyHeld := make(chan bool, 2)
	go func() {
		wasScheduled.Done() // Got scheduled!
		s.cl.messagesLock.Lock()
		defer s.cl.messagesLock.Unlock()
		probablyHeld <- false
	}()
	wasScheduled.Wait()
	select {
	case <-time.After(100 * time.Millisecond):
		probablyHeld <- true
	}
	c.Assert(<-probablyHeld, Equals, true)

	// Now terminate, this should return and work and not be claimed
	c.Assert(s.cl.Terminate(), Equals, true)
	c.Assert(s.cl.Claimed(), Equals, true)
}

func (s *ClaimSuite) TestCommitOutstanding(c *C) {
	// Test that calling CommitOffsets should commit offsets for outstanding messages and
	// updates claim tracking
	c.Assert(s.Produce("test16", 0, "m1", "m2", "m3", "m4", "m5", "m6"), Equals, int64(5))
	c.Assert(s.cl.updateOffsets(), IsNil)
	c.Assert(s.cl.offsets.Current, Equals, int64(0))
	c.Assert(s.cl.offsets.Earliest, Equals, int64(0))
	c.Assert(s.cl.offsets.Latest, Equals, int64(6))

	// This test requires all messages to have been consumed into the channel, else
	// we can get inconsistent results
	readyChan := make(chan struct{})
	go func() {
		defer close(readyChan)
		for {
			s.cl.lock.RLock()
			if len(s.cl.messages) == 6 {
				s.cl.lock.RUnlock()
				break
			}
			s.cl.lock.RUnlock()

			time.Sleep(100 * time.Millisecond)
		}
	}()
	select {
	case <-readyChan:
		// all good, continue
	case <-time.After(3 * time.Second):
		// Timeout reached, we've failed
		c.FailNow()
	}

	// Consume 1, heartbeat... offsets still 0
	msg1 := s.consumeOne(c)
	c.Assert(msg1.Value, DeepEquals, []byte("m1"))
	c.Assert(s.cl.Commit(msg1.Offset), IsNil)
	c.Assert(s.cl.numTrackingOffsets(), Equals, 6)
	c.Assert(s.cl.offsets.Current, Equals, int64(0))

	// Commit the offsets....should update current offset and tracking for the claim
	c.Assert(s.cl.Terminate(), Equals, true)
	c.Assert(s.cl.offsets.Current, Equals, int64(1))
	c.Assert(s.cl.numTrackingOffsets(), Equals, 5)
}

func (s *ClaimSuite) TestCurrentLag(c *C) {
	// Test that GetCurrentLag returns the correct numbers in various cases
	s.cl.offsets.Current = 0
	s.cl.offsets.Latest = 0
	c.Assert(s.cl.GetCurrentLag(), Equals, int64(0))

	s.cl.offsets.Current = 1
	s.cl.offsets.Latest = 0
	c.Assert(s.cl.GetCurrentLag(), Equals, int64(0))

	s.cl.offsets.Current = 0
	s.cl.offsets.Latest = 1
	c.Assert(s.cl.GetCurrentLag(), Equals, int64(1))

	s.cl.offsets.Current = 1
	s.cl.offsets.Latest = 2
	c.Assert(s.cl.GetCurrentLag(), Equals, int64(1))
}

func (s *ClaimSuite) TestHeartbeat(c *C) {
	// Ensure that our heartbeats are updating the marshal structures appropriately
	// (makes sure clients are seeing the right values)
	c.Assert(s.m.GetPartitionClaim("test16", 0).CurrentOffset, Equals, int64(0))
	s.cl.offsets.Current = 10
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.m.cluster.waitForRsteps(3), Equals, 3)
	c.Assert(s.m.GetPartitionClaim("test16", 0).CurrentOffset, Equals, int64(10))

	// And test that releasing means we can't update heartbeat anymore
	c.Assert(s.cl.Release(), Equals, true)
	c.Assert(s.m.cluster.waitForRsteps(4), Equals, 4)
	s.cl.offsets.Current = 20
	c.Assert(s.cl.heartbeat(), Equals, false)
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastHeartbeat, Equals, int64(0))
	c.Assert(s.m.GetPartitionClaim("test16", 0).CurrentOffset, Equals, int64(0))
	c.Assert(s.m.GetLastPartitionClaim("test16", 0).CurrentOffset, Equals, int64(10))
}

func (s *ClaimSuite) TestVelocity(c *C) {
	// Test that the velocity functions perform as expected given the expected inputs
	s.cl.offsetCurrentHistory = [10]int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	c.Assert(s.cl.ConsumerVelocity(), Equals, float64(0))
	s.cl.offsetLatestHistory = s.cl.offsetCurrentHistory
	c.Assert(s.cl.PartitionVelocity(), Equals, s.cl.ConsumerVelocity())

	s.cl.offsetCurrentHistory = [10]int64{1, 2, 0, 0, 0, 0, 0, 0, 0, 0}
	c.Assert(s.cl.ConsumerVelocity(), Equals, float64(1))
	s.cl.offsetLatestHistory = s.cl.offsetCurrentHistory
	c.Assert(s.cl.PartitionVelocity(), Equals, s.cl.ConsumerVelocity())

	s.cl.offsetCurrentHistory = [10]int64{1, 2, 3, 0, 0, 0, 0, 0, 0, 0}
	c.Assert(s.cl.ConsumerVelocity(), Equals, float64(1))
	s.cl.offsetLatestHistory = s.cl.offsetCurrentHistory
	c.Assert(s.cl.PartitionVelocity(), Equals, s.cl.ConsumerVelocity())

	s.cl.offsetCurrentHistory = [10]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	c.Assert(s.cl.ConsumerVelocity(), Equals, float64(1))
	s.cl.offsetLatestHistory = s.cl.offsetCurrentHistory
	c.Assert(s.cl.PartitionVelocity(), Equals, s.cl.ConsumerVelocity())

	s.cl.offsetCurrentHistory = [10]int64{1, 21, 21, 0, 0, 0, 0, 0, 0, 0}
	c.Assert(s.cl.ConsumerVelocity(), Equals, float64(10))
	s.cl.offsetLatestHistory = s.cl.offsetCurrentHistory
	c.Assert(s.cl.PartitionVelocity(), Equals, s.cl.ConsumerVelocity())

	s.cl.offsetCurrentHistory = [10]int64{1, 21, 21, 21, 21, 0, 0, 0, 0, 0}
	c.Assert(s.cl.ConsumerVelocity(), Equals, float64(5))
	s.cl.offsetLatestHistory = s.cl.offsetCurrentHistory
	c.Assert(s.cl.PartitionVelocity(), Equals, s.cl.ConsumerVelocity())

	s.cl.offsetCurrentHistory = [10]int64{21, 21, 1, 21, 21, 0, 0, 0, 0, 0}
	c.Assert(s.cl.ConsumerVelocity(), Equals, float64(5))
	s.cl.offsetLatestHistory = s.cl.offsetCurrentHistory
	c.Assert(s.cl.PartitionVelocity(), Equals, s.cl.ConsumerVelocity())

	s.cl.offsetCurrentHistory = [10]int64{21, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	c.Assert(s.cl.ConsumerVelocity(), Equals, float64(0))
	s.cl.offsetLatestHistory = s.cl.offsetCurrentHistory
	c.Assert(s.cl.PartitionVelocity(), Equals, s.cl.ConsumerVelocity())
}

func (s *ClaimSuite) TestHealthCheck(c *C) {
	// Ensure that the health check system returns expected values for given states
	s.cl.offsets.Current = 0
	s.cl.offsetCurrentHistory = [10]int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	s.cl.offsets.Latest = 0
	s.cl.offsetLatestHistory = [10]int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	c.Assert(s.cl.healthCheck(), Equals, true)
	c.Assert(s.cl.cyclesBehind, Equals, 0)

	// Put us in an "unhealthy" state, PV is high and we aren't caught up
	s.cl.offsets.Latest = 10
	s.cl.offsetLatestHistory = [10]int64{1, 10, 0, 0, 0, 0, 0, 0, 0, 0}
	c.Assert(s.cl.healthCheck(), Equals, true)
	c.Assert(s.cl.cyclesBehind, Equals, 1)

	// Now we're "caught up" even PV>CV we're healthy
	s.cl.offsets.Current = 21
	s.cl.offsetCurrentHistory = [10]int64{1, 6, 11, 16, 21, 0, 0, 0, 0, 0}
	s.cl.offsets.Latest = 21
	s.cl.offsetLatestHistory = [10]int64{1, 11, 21, 0, 0, 0, 0, 0, 0, 0}
	c.Assert(s.cl.ConsumerVelocity() < s.cl.PartitionVelocity(), Equals, true)
	c.Assert(s.cl.healthCheck(), Equals, true)
	c.Assert(s.cl.cyclesBehind, Equals, 0)

	// Test that "predictive speed" is working, i.e., that the consumer is
	// considered healthy when it's within a heartbeat of the end
	s.cl.offsets.Latest = 31
	s.cl.offsetLatestHistory = [10]int64{1, 11, 21, 31, 0, 0, 0, 0, 0, 0}
	c.Assert(s.cl.ConsumerVelocity() < s.cl.PartitionVelocity(), Equals, true)
	c.Assert(s.cl.healthCheck(), Equals, true)
	c.Assert(s.cl.cyclesBehind, Equals, 0)

	// Now we're behind and fail health checks 3 times, this will release
	s.cl.offsets.Latest = 32
	s.cl.offsetLatestHistory = [10]int64{1, 11, 21, 32, 0, 0, 0, 0, 0, 0}
	c.Assert(s.cl.ConsumerVelocity() < s.cl.PartitionVelocity(), Equals, true)
	c.Assert(s.cl.healthCheck(), Equals, true)
	c.Assert(s.cl.cyclesBehind, Equals, 1)
	c.Assert(s.cl.healthCheck(), Equals, true)
	c.Assert(s.cl.cyclesBehind, Equals, 2)
	c.Assert(s.cl.healthCheck(), Equals, false)
	c.Assert(s.cl.cyclesBehind, Equals, 3)
	c.Assert(s.m.cluster.waitForRsteps(3), Equals, 3)
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastHeartbeat, Equals, int64(0))
	c.Assert(s.m.GetPartitionClaim("test16", 0).CurrentOffset, Equals, int64(0))
	c.Assert(s.m.GetLastPartitionClaim("test16", 0).CurrentOffset, Equals, int64(21))

	// If we are okay with CV<PV we shouldn't release
	opts := NewConsumerOptions()
	opts.ReleaseClaimsIfBehind = false
	s.cl = newClaim("test16", 0, s.m, nil, s.ch, opts)
	c.Assert(s.cl, NotNil)
	s.cl.offsetLatestHistory = [10]int64{1, 10, 0, 0, 0, 0, 0, 0, 0, 0}
	s.cl.offsetCurrentHistory = [10]int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	s.cl.offsets.Current = 0
	s.cl.offsets.Latest = 10
	c.Assert(s.cl.ConsumerVelocity() < s.cl.PartitionVelocity(), Equals, true)
	c.Assert(s.cl.healthCheck(), Equals, true)
}

func (s *ClaimSuite) TestHealthCheckRelease(c *C) {
	// Test that an expired heartbeat causes the partition to get immediately released
	s.cl.lastHeartbeat -= HeartbeatInterval * 2
	s.cl.offsets.Current = 5
	c.Assert(s.cl.healthCheck(), Equals, false)
	c.Assert(s.m.cluster.waitForRsteps(3), Equals, 3)
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastHeartbeat, Equals, int64(0))
	s.assertRelease(c)
	c.Assert(s.cl.healthCheck(), Equals, false)
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastHeartbeat, Equals, int64(0))
	c.Assert(s.m.GetPartitionClaim("test16", 0).CurrentOffset, Equals, int64(0))
	c.Assert(s.m.GetLastPartitionClaim("test16", 0).CurrentOffset, Equals, int64(5))
}
