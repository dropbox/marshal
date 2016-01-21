package marshal

import (
	"time"

	. "gopkg.in/check.v1"

	"github.com/optiopay/kafka/kafkatest"
	"github.com/optiopay/kafka/proto"
)

var _ = Suite(&ClaimSuite{})

type ClaimSuite struct {
	c  *C
	s  *kafkatest.Server
	m  *Marshaler
	ch chan *proto.Message
	cl *claim
}

func (s *ClaimSuite) SetUpTest(c *C) {
	s.c = c
	s.s = StartServer()
	s.ch = make(chan *proto.Message, 10)
	var err error
	s.m, err = NewMarshaler("cl", "gr", []string{s.s.Addr()})
	c.Assert(err, IsNil)
	s.cl = newClaim("test16", 0, s.m, s.ch, NewConsumerOptions())
	s.cl.startMessagePump()
}

func (s *ClaimSuite) TearDownTest(c *C) {
	s.cl.Release()
	s.m.Terminate()
	s.s.Close()
}

func (s *ClaimSuite) Produce(topicName string, partID int, msgs ...string) int64 {
	var protos []*proto.Message
	for _, msg := range msgs {
		protos = append(protos, &proto.Message{Value: []byte(msg)})
	}
	offset, err := s.m.producer.Produce(topicName, int32(partID), protos...)
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

func (s *ClaimSuite) consumeOne(c *C) *proto.Message {
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
	c.Assert(s.cl.tracking[0], Equals, false)

	// Consume 2, still 0
	msg2 := s.consumeOne(c)
	c.Assert(msg2.Value, DeepEquals, []byte("m2"))
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.cl.offsets.Current, Equals, int64(0))

	// Commit 1, offset 1 but only after heartbeat phase
	c.Assert(s.cl.Commit(msg1), IsNil)
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
	c.Assert(s.cl.Commit(msg3), IsNil)
	c.Assert(s.cl.offsets.Current, Equals, int64(1))
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.cl.offsets.Current, Equals, int64(1))
	c.Assert(s.cl.numTrackingOffsets(), Equals, 5)

	// Commit #2, offset now advances to 3
	c.Assert(s.cl.Commit(msg2), IsNil)
	c.Assert(s.cl.offsets.Current, Equals, int64(1))
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.cl.offsets.Current, Equals, int64(3))
	c.Assert(s.cl.numTrackingOffsets(), Equals, 3)

	// Attempt to commit invalid offset (never seen), make sure it errors
	msg3.Offset = 95
	c.Assert(s.cl.Commit(msg3), NotNil)

	// Commit the rest
	c.Assert(s.cl.Commit(s.consumeOne(c)), IsNil)
	c.Assert(s.cl.Commit(s.consumeOne(c)), IsNil)
	c.Assert(s.cl.Commit(s.consumeOne(c)), IsNil)
	c.Assert(s.cl.offsets.Current, Equals, int64(3))
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.cl.offsets.Current, Equals, int64(6))
	c.Assert(s.cl.numTrackingOffsets(), Equals, 0)
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
	c.Assert(s.cl.tracking[0], Equals, false)

	// Attempt to consume a message, but we expect it to fail (i.e. no message to be
	// available)
	select {
	case <-s.ch:
		c.Error("Expected no message, but message was returned")
	case <-time.After(300 * time.Millisecond):
		// Good.
	}

	// Now commit, and then try to consume again, it should work
	c.Assert(s.cl.Commit(msg1), IsNil)
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
			s.cl.Commit(msg)
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
			s.cl.Commit(msg)
		}
	}
}

func (s *ClaimSuite) TestRelease(c *C) {
	// Test that calling Release on a claim properly sets the flag and releases the
	// partition
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastHeartbeat, Not(Equals), int64(0))
	c.Assert(s.cl.Claimed(), Equals, true)
	c.Assert(s.cl.Release(), Equals, true)
	c.Assert(s.cl.Claimed(), Equals, false)
	c.Assert(s.m.waitForRsteps(3), Equals, 3)
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

func (s *ClaimSuite) TestCommitOutstanding(c *C) {
	// Test that calling CommitOffsets should commit offsets for outstanding messages and
	// updates claim tracking
	c.Assert(s.Produce("test16", 0, "m1", "m2", "m3", "m4", "m5", "m6"), Equals, int64(5))
	c.Assert(s.cl.updateOffsets(), IsNil)
	c.Assert(s.cl.offsets.Current, Equals, int64(0))
	c.Assert(s.cl.offsets.Earliest, Equals, int64(0))
	c.Assert(s.cl.offsets.Latest, Equals, int64(6))

	// Consume 1, heartbeat... offsets still 0
	msg1 := s.consumeOne(c)
	c.Assert(msg1.Value, DeepEquals, []byte("m1"))
	c.Assert(s.cl.Commit(msg1), IsNil)
	// TODO: There's a race here. If the background goroutine decides to run a health check
	// here it will consume our offset and this test fails. It's rare but possible.
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
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastOffset, Equals, int64(0))
	s.cl.offsets.Current = 10
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.m.waitForRsteps(3), Equals, 3)
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastOffset, Equals, int64(10))

	// And test that releasing means we can't update heartbeat anymore
	c.Assert(s.cl.Release(), Equals, true)
	c.Assert(s.m.waitForRsteps(4), Equals, 4)
	s.cl.offsets.Current = 20
	c.Assert(s.cl.heartbeat(), Equals, false)
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastHeartbeat, Equals, int64(0))
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastOffset, Equals, int64(0))
	c.Assert(s.m.GetLastPartitionClaim("test16", 0).LastOffset, Equals, int64(10))
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

	// Now we're behind and fail health checks 3 times, this will release
	s.cl.offsets.Latest = 31
	s.cl.offsetLatestHistory = [10]int64{1, 11, 21, 31, 0, 0, 0, 0, 0, 0}
	c.Assert(s.cl.ConsumerVelocity() < s.cl.PartitionVelocity(), Equals, true)
	c.Assert(s.cl.healthCheck(), Equals, true)
	c.Assert(s.cl.cyclesBehind, Equals, 1)
	c.Assert(s.cl.healthCheck(), Equals, true)
	c.Assert(s.cl.cyclesBehind, Equals, 2)
	c.Assert(s.cl.healthCheck(), Equals, false)
	c.Assert(s.cl.cyclesBehind, Equals, 3)
	c.Assert(s.m.waitForRsteps(3), Equals, 3)
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastHeartbeat, Equals, int64(0))
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastOffset, Equals, int64(0))
	c.Assert(s.m.GetLastPartitionClaim("test16", 0).LastOffset, Equals, int64(21))
}

func (s *ClaimSuite) TestHealthCheckRelease(c *C) {
	// Test that an expired heartbeat causes the partition to get immediately released
	s.cl.lastHeartbeat -= HeartbeatInterval * 2
	s.cl.offsets.Current = 5
	c.Assert(s.cl.healthCheck(), Equals, false)
	c.Assert(s.m.waitForRsteps(3), Equals, 3)
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastHeartbeat, Equals, int64(0))
	c.Assert(s.cl.Claimed(), Equals, false)
	c.Assert(s.cl.healthCheck(), Equals, false)
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastHeartbeat, Equals, int64(0))
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastOffset, Equals, int64(0))
	c.Assert(s.m.GetLastPartitionClaim("test16", 0).LastOffset, Equals, int64(5))
}
