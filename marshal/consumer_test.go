package marshal

import (
	"github.com/optiopay/kafka/kafkatest"
	"github.com/optiopay/kafka/proto"
	. "gopkg.in/check.v1"
	"math/rand"
	"sort"
	"strconv"
	"sync/atomic"
	"time"
)

var _ = Suite(&ConsumerSuite{})

type ConsumerSuite struct {
	c  *C
	s  *kafkatest.Server
	m  *Marshaler
	m2 *Marshaler
	cn *Consumer
}

func (s *ConsumerSuite) NewTestConsumer(m *Marshaler, topics []string) *Consumer {
	cn := &Consumer{
		alive:      new(int32),
		marshal:    m,
		topics:     topics,
		options:    NewConsumerOptions(),
		partitions: make(map[string]int),
		rand:       rand.New(rand.NewSource(time.Now().UnixNano())),
		claims:     make(map[string]map[int]*claim),
		messages:   make(chan *proto.Message, 1000),
	}

	for _, topic := range topics {
		cn.partitions[topic] = m.Partitions(topic)
	}
	atomic.StoreInt32(cn.alive, 1)
	return cn
}

func (s *ConsumerSuite) SetUpTest(c *C) {
	s.c = c
	s.s = StartServer()

	var err error
	s.m, err = NewMarshaler("cl", "gr", []string{s.s.Addr()})
	c.Assert(err, IsNil)
	s.m2, err = NewMarshaler("cl2", "gr", []string{s.s.Addr()})
	c.Assert(err, IsNil)

	s.cn = s.NewTestConsumer(s.m, []string{"test16"})
}

func (s *ConsumerSuite) TearDownTest(c *C) {
	if s.cn != nil {
		s.cn.Terminate(true)
	}
	s.m.Terminate()
	s.s.Close()
}

func (s *ConsumerSuite) Produce(topicName string, partID int, msgs ...string) int64 {
	var protos []*proto.Message
	for _, msg := range msgs {
		protos = append(protos, &proto.Message{Value: []byte(msg)})
	}
	offset, err := s.m.producer.Produce(topicName, int32(partID), protos...)
	s.c.Assert(err, IsNil)
	return offset
}

func (s *ConsumerSuite) TestNewConsumer(c *C) {
	options := NewConsumerOptions()
	options.GreedyClaims = true

	cn, err := s.m.NewConsumer([]string{"test1"}, options)
	c.Assert(err, IsNil)
	defer cn.Terminate(true)

	// Wait for 2 messages to be processed
	c.Assert(s.m.waitForRsteps(2), Equals, 2)
	c.Assert(s.m.GetPartitionClaim("test1", 0).LastHeartbeat, Not(Equals), int64(0))

	// Test basic consumption
	s.Produce("test1", 0, "m1", "m2", "m3")
	c.Assert(cn.consumeOne().Value, DeepEquals, []byte("m1"))
	c.Assert(cn.consumeOne().Value, DeepEquals, []byte("m2"))
	c.Assert(cn.consumeOne().Value, DeepEquals, []byte("m3"))

	// Get consumer channel for this next test
	chn := cn.ConsumeChannel()

	// Terminate marshaler, ensure it terminates the consumer
	s.m.Terminate()
	c.Assert(s.m.Terminated(), Equals, true)
	c.Assert(cn.Terminated(), Equals, true)

	// Ensure that the channel has been closed
	select {
	case _, ok := <-chn:
		c.Assert(ok, Equals, false)
	default:
		c.Assert(false, Equals, true)
	}

	// Now ensure we can't create a new consumer
	cn, err = s.m.NewConsumer([]string{"test1"}, options)
	c.Assert(cn, IsNil)
	c.Assert(err, NotNil)
}

func (s *ConsumerSuite) TestTerminateWithRelease(c *C) {
	// Termination is supposed to release active claims that we have, ensure that
	// this happens
	c.Assert(s.cn.tryClaimPartition(s.cn.defaultTopic(), 0), Equals, true)
	c.Assert(s.cn.Terminate(true), Equals, true)
	c.Assert(s.m.waitForRsteps(3), Equals, 3)
	c.Assert(s.m.GetPartitionClaim(s.cn.defaultTopic(), 0).LastHeartbeat, Equals, int64(0))
}

func (s *ConsumerSuite) TestTerminateWithoutRelease(c *C) {
	// Termination is supposed to commit the active claims without releasing the partition
	c.Assert(s.cn.tryClaimPartition(s.cn.defaultTopic(), 0), Equals, true)
	c.Assert(s.cn.Terminate(false), Equals, true)
	// Shouldn't release the partition
	c.Assert(s.m.GetPartitionClaim(s.cn.defaultTopic(), 0).LastHeartbeat, Not(Equals), int64(0))
}

func (s *ConsumerSuite) TestMultiClaim(c *C) {
	// Set up claims on two partitions, we'll put messages in both and then ensure that
	// we get all of the messages out
	c.Assert(s.cn.tryClaimPartition(s.cn.defaultTopic(), 0), Equals, true)
	c.Assert(s.cn.tryClaimPartition(s.cn.defaultTopic(), 1), Equals, true)
	numMessages := 100
	// Produce numMessages messages to the two partitions
	for i := 0; i < numMessages; i++ {
		s.Produce("test16", i%2, strconv.Itoa(i))
	}

	// Now consume numMessages times and ensure we get exactly 1000 unique messages
	results := make(map[string]bool)
	for i := 0; i < numMessages; i++ {
		results[string(s.cn.consumeOne().Value)] = true
	}
	c.Assert(len(results), Equals, numMessages)
}

func (s *ConsumerSuite) TestTopicClaim(c *C) {
	topic := "test2"
	// Claim an entire topic
	options := NewConsumerOptions()
	options.ClaimEntireTopic = true
	cn, err := s.m.NewConsumer([]string{topic}, options)
	c.Assert(err, IsNil)
	defer cn.Terminate(true)

	// Wait for 4 messages to be processed and ensure we have the entire topic
	c.Assert(s.m.waitForRsteps(4), Equals, 4)
	c.Assert(s.m.GetPartitionClaim(topic, 0).LastHeartbeat, Not(Equals), int64(0))
	c.Assert(s.m.GetPartitionClaim(topic, 1).LastHeartbeat, Not(Equals), int64(0))
}

func (s *ConsumerSuite) TestTopicClaimBlocked(c *C) {
	topic := "test2"
	// Claim partition 0 with one consumer
	cnbl := s.NewTestConsumer(s.m, []string{topic})
	c.Assert(cnbl.tryClaimPartition(topic, 0), Equals, true)
	c.Assert(s.m.waitForRsteps(2), Equals, 2)
	c.Assert(s.m2.waitForRsteps(2), Equals, 2)

	// Claim an entire topic, this creates a real consumer
	cn := s.NewTestConsumer(s.m2, []string{topic})
	cn.options.ClaimEntireTopic = true
	defer cn.Terminate(true)

	// Force our consumer to run it's topic claim loop so we know it has run
	cn.claimTopics()

	// Ensure partition 1 is unclaimed still and that our new consumer has no claims
	c.Assert(s.m.GetPartitionClaim(topic, 1).LastHeartbeat, Equals, int64(0))
	c.Assert(cnbl.getNumActiveClaims(), Equals, 1)
	c.Assert(cn.getNumActiveClaims(), Equals, 0)

	// Now release partition 0
	cnbl.Terminate(true)
	c.Assert(s.m.waitForRsteps(3), Equals, 3)
	c.Assert(s.m2.waitForRsteps(3), Equals, 3)
	c.Assert(cnbl.getNumActiveClaims(), Equals, 0)
	c.Assert(cn.getNumActiveClaims(), Equals, 0)

	// Reclaim and assert we get two claims
	cn.claimTopics()
	c.Assert(cnbl.getNumActiveClaims(), Equals, 0)
	c.Assert(cn.getNumActiveClaims(), Equals, 2)
}

func (s *ConsumerSuite) TestTopicClaimPartial(c *C) {
	topic := "test2"
	// Claim partition 1 with one consumer
	cnbl := s.NewTestConsumer(s.m, []string{topic})
	c.Assert(cnbl.tryClaimPartition(topic, 1), Equals, true)
	c.Assert(s.m.waitForRsteps(2), Equals, 2)
	c.Assert(s.m2.waitForRsteps(2), Equals, 2)

	// Claim an entire topic, this creates a real consumer
	cn := s.NewTestConsumer(s.m2, []string{topic})
	cn.options.ClaimEntireTopic = true
	defer cn.Terminate(true)

	// Force our consumer to run it's topic claim loop so we know it has run
	cn.claimTopics()

	// Both should have 1 partition -- the topic claim got 0, and the other one still has 1.
	// This can happen in the case where a consumer has died and partition 0's claim expires
	// before the other partitions.
	c.Assert(cnbl.getNumActiveClaims(), Equals, 1)
	c.Assert(cn.getNumActiveClaims(), Equals, 1)

	// Now release partition 1, end state should be that the topic claimant still has 0 and
	// nobody has 1
	cnbl.Terminate(true)
	c.Assert(s.m.waitForRsteps(5), Equals, 5)
	c.Assert(s.m2.waitForRsteps(5), Equals, 5)
	c.Assert(cnbl.getNumActiveClaims(), Equals, 0)
	c.Assert(cn.getNumActiveClaims(), Equals, 1)

	// Now the topic claimant runs again and will see it can claim partition 1 and does
	cn.claimTopics()
	c.Assert(s.m.waitForRsteps(7), Equals, 7)
	c.Assert(s.m2.waitForRsteps(7), Equals, 7)
	c.Assert(cnbl.getNumActiveClaims(), Equals, 0)
	c.Assert(cn.getNumActiveClaims(), Equals, 2)
}

func (s *ConsumerSuite) TestMultiTopicClaim(c *C) {
	// Claim partition 1 with one consumer
	topics := []string{"test1", "test2", "test16"}
	cn := s.NewTestConsumer(s.m, topics)
	cn.options.ClaimEntireTopic = true
	defer cn.Terminate(true)

	// Force our consumer to run it's topic claim loop so we know it has run
	cn.claimTopics()

	// we should have claimed all topic partitions
	partitions := 0
	for _, topic := range topics {
		partitions += s.m.Partitions(topic)
	}
	c.Assert(cn.getNumActiveClaims(), Equals, partitions)
}

func (s *ConsumerSuite) TestMultiTopicClaimWithLimit(c *C) {
	// Claim partition 1 with one consumer
	topics := []string{"test1", "test2", "test16"}
	cn := s.NewTestConsumer(s.m, topics)
	cn.options.ClaimEntireTopic = true
	cn.options.MaximumClaims = 2
	defer cn.Terminate(true)

	// Force our consumer to run it's topic claim loop so we know it has run
	cn.claimTopics()
	claimed := 0
	claimed += s.m.Partitions(topics[0])
	claimed += s.m.Partitions(topics[1])
	c.Assert(cn.getNumActiveClaims(), Equals, claimed)
}

func (s *ConsumerSuite) TestUnhealthyPartition(c *C) {
	c.Assert(s.cn.tryClaimPartition(s.cn.defaultTopic(), 0), Equals, true)
	s.cn.lock.RLock()
	cl := s.cn.claims[s.cn.defaultTopic()][0]
	s.cn.lock.RUnlock()

	// We just claimed, nothing should be unhealthy
	c.Assert(cl.healthCheck(), Equals, true)
	c.Assert(cl.cyclesBehind, Equals, 0)

	// Put in one message and consume it making sure things work, and then update offsets.
	s.Produce("test16", 0, "m1")
	c.Assert(s.cn.consumeOne().Value, DeepEquals, []byte("m1"))
	s.cn.claims[s.cn.defaultTopic()][0].heartbeat()
	c.Assert(cl.updateOffsets(), IsNil)
	c.Assert(cl.healthCheck(), Equals, true)
	c.Assert(cl.cyclesBehind, Equals, 0)

	// Produce 5, consume 3... at this point we are "unhealthy" since we are falling
	// behind for this period
	s.Produce("test16", 0, "m2", "m3", "m4", "m5", "m6")
	c.Assert(s.cn.consumeOne().Value, DeepEquals, []byte("m2"))
	c.Assert(s.cn.consumeOne().Value, DeepEquals, []byte("m3"))
	c.Assert(s.cn.consumeOne().Value, DeepEquals, []byte("m4"))
	s.cn.claims[s.cn.defaultTopic()][0].heartbeat()
	c.Assert(cl.updateOffsets(), IsNil)
	c.Assert(cl.healthCheck(), Equals, true)
	c.Assert(cl.cyclesBehind, Equals, 1)

	// Produce nothing and consume the last two, we become healthy again because
	// we are caught up and our velocity is equal
	c.Assert(s.cn.consumeOne().Value, DeepEquals, []byte("m5"))
	c.Assert(s.cn.consumeOne().Value, DeepEquals, []byte("m6"))
	s.cn.claims[s.cn.defaultTopic()][0].heartbeat()
	c.Assert(cl.updateOffsets(), IsNil)
	c.Assert(cl.healthCheck(), Equals, true)
	c.Assert(cl.ConsumerVelocity() == cl.PartitionVelocity(), Equals, true)
	c.Assert(cl.cyclesBehind, Equals, 0)

	// Produce again, falls behind slightly
	s.Produce("test16", 0, "m7")
	c.Assert(cl.updateOffsets(), IsNil)
	c.Assert(cl.healthCheck(), Equals, true)
	c.Assert(cl.cyclesBehind, Equals, 1)

	// Still behind
	c.Assert(cl.updateOffsets(), IsNil)
	c.Assert(cl.healthCheck(), Equals, true)
	c.Assert(cl.cyclesBehind, Equals, 2)

	// Consume the last message, which will fix our velocity and let us
	// pass as healthy again
	c.Assert(s.cn.consumeOne().Value, DeepEquals, []byte("m7"))
	s.cn.claims[s.cn.defaultTopic()][0].heartbeat()
	c.Assert(cl.updateOffsets(), IsNil)
	c.Assert(cl.healthCheck(), Equals, true)
	c.Assert(cl.cyclesBehind, Equals, 0)
}

func (s *ConsumerSuite) TestConsumerHeartbeat(c *C) {
	c.Assert(s.cn.tryClaimPartition(s.cn.defaultTopic(), 0), Equals, true)
	c.Assert(s.m.waitForRsteps(2), Equals, 2)
	cl := s.cn.claims[s.cn.defaultTopic()][0]
	// Newly claimed partition should have heartbeated
	c.Assert(cl.lastHeartbeat, Not(Equals), 0)

	// Now reset the heartbeat to some other value
	cl.lastHeartbeat -= HeartbeatInterval
	hb := cl.lastHeartbeat

	// Manual heartbeat, ensure lastHeartbeat is updated
	cl.heartbeat()
	c.Assert(s.m.waitForRsteps(3), Equals, 3)
	c.Assert(cl.lastHeartbeat, Not(Equals), hb)
}

func (s *ConsumerSuite) TestCommittedOffset(c *C) {
	// Test that we save/load committed offsets properly and that when we load them we will
	// prefer them over the heartbeated values (if they're higher)
	s.Produce("test16", 0, "m1", "m2", "m3", "m4")
	c.Assert(s.m.offsets.Commit("test16", 0, 2), IsNil)
	c.Assert(s.cn.tryClaimPartition(s.cn.defaultTopic(), 0), Equals, true)
	c.Assert(s.m.waitForRsteps(2), Equals, 2)
	cl := s.cn.claims[s.cn.defaultTopic()][0]
	c.Assert(cl.offsets.Current, Equals, int64(2))

	// Since the committed offset was 2, the first consumption should be the third message
	c.Assert(s.cn.consumeOne().Value, DeepEquals, []byte("m3"))

	// Heartbeat should succeed and update the committed offset
	c.Assert(cl.heartbeat(), Equals, true)
	c.Assert(s.m.waitForRsteps(3), Equals, 3)
	offset, _, err := s.m.offsets.Offset("test16", 0)
	c.Assert(err, IsNil)
	c.Assert(offset, Equals, int64(3))
	c.Assert(cl.Release(), Equals, true)
	c.Assert(s.m.waitForRsteps(4), Equals, 4)
	c.Assert(cl.Claimed(), Equals, false)
	s.cn.claims[s.cn.defaultTopic()][0] = nil

	// Now let's "downcommit" the offset back to an earlier value, and then re-claim the
	// partition to verify that it sets the offset to the heartbeated value rather than
	// the committed value
	c.Assert(s.m.offsets.Commit("test16", 0, 2), IsNil)
	offset, _, err = s.m.offsets.Offset("test16", 0)
	c.Assert(err, IsNil)
	c.Assert(offset, Equals, int64(2))
	c.Assert(s.cn.tryClaimPartition(s.cn.defaultTopic(), 0), Equals, true)
	c.Assert(s.m.waitForRsteps(6), Equals, 6)
	c.Assert(s.cn.claims[s.cn.defaultTopic()][0].offsets.Current, Equals, int64(3))
}

func (s *ConsumerSuite) TestStrictOrdering(c *C) {
	// Test that we can strict ordering semantics
	s.cn.options.StrictOrdering = true
	s.Produce("test16", 0, "m1", "m2", "m3", "m4")
	s.Produce("test16", 1, "m1", "m2", "m3", "m4")
	c.Assert(s.cn.tryClaimPartition(s.cn.defaultTopic(), 0), Equals, true)
	c.Assert(s.cn.tryClaimPartition(s.cn.defaultTopic(), 1), Equals, true)
	c.Assert(s.m.waitForRsteps(4), Equals, 4)

	msg1 := <-s.cn.messages
	c.Assert(msg1.Value, DeepEquals, []byte("m1"))
	msg2 := <-s.cn.messages
	c.Assert(msg2.Value, DeepEquals, []byte("m1"))

	// This should time out (no messages available)
	select {
	case <-s.cn.messages:
		c.Error("Expected timeout, got message.")
	case <-time.After(300 * time.Millisecond):
		// Nothing, this is good.
	}

	// Commit the first message, expect a single new message
	c.Assert(s.cn.Commit(msg1), IsNil)
	msg3 := <-s.cn.messages
	c.Assert(msg3.Value, DeepEquals, []byte("m2"))

	// This should time out (no messages available)
	select {
	case <-s.cn.messages:
		c.Error("Expected timeout, got message.")
	case <-time.After(300 * time.Millisecond):
		// Nothing, this is good.
	}
}

func (s *ConsumerSuite) TestTryClaimPartition(c *C) {
	// Should work
	c.Assert(s.cn.tryClaimPartition(s.cn.defaultTopic(), 0), Equals, true)
	// Should fail (can't claim a second time)
	c.Assert(s.cn.tryClaimPartition(s.cn.defaultTopic(), 0), Equals, false)
}

func (s *ConsumerSuite) TestAggressiveClaim(c *C) {
	// Ensure aggressive mode claims all partitions in a single call to claim
	s.cn.options.GreedyClaims = true
	c.Assert(s.cn.GetCurrentLoad(), Equals, 0)
	s.cn.claimPartitions()
	c.Assert(s.cn.GetCurrentLoad(), Equals, 16)
}

func (s *ConsumerSuite) TestBalancedClaim(c *C) {
	// Ensure balanced mode only claims one partition
	c.Assert(s.cn.GetCurrentLoad(), Equals, 0)
	s.cn.claimPartitions()
	c.Assert(s.cn.GetCurrentLoad(), Equals, 1)
}

func (s *ConsumerSuite) TestFastReclaim(c *C) {
	// Claim some partitions then create a new consumer with fast reclaim on; this
	// should "reclaim" the partitions automatically at the offset they were last
	// reported at
	cn1, err := s.m.NewConsumer([]string{"test2"}, NewConsumerOptions())
	c.Assert(err, IsNil)
	defer cn1.Terminate(true)
	s.Produce("test2", 0, "m1", "m2", "m3")

	// By default the consumer will claim all partitions so let's wait for that
	c.Assert(s.m.waitForRsteps(4), Equals, 4)
	cn1.lock.RLock()
	cn1.lock.RUnlock()

	// Consume the first two messages from 0, then heartbeat to set the offset to 2
	c.Assert(cn1.consumeOne().Value, DeepEquals, []byte("m1"))
	c.Assert(cn1.consumeOne().Value, DeepEquals, []byte("m2"))
	c.Assert(cn1.claims["test2"][0].heartbeat(), Equals, true)
	c.Assert(s.m.waitForRsteps(5), Equals, 5)

	// Now add some messages to the next, but only consume some
	s.Produce("test2", 1, "p1", "p2", "p3", "p4")
	c.Assert(cn1.consumeOne().Value, DeepEquals, []byte("m3"))
	c.Assert(cn1.consumeOne().Value, DeepEquals, []byte("p1"))
	c.Assert(cn1.consumeOne().Value, DeepEquals, []byte("p2"))
	c.Assert(cn1.consumeOne().Value, DeepEquals, []byte("p3"))

	// Now we "reclaim" by creating a new consumer here; this is actually bogus
	// usage as it would normally lead to stepping on the prior consumer, but it
	// is useful for this test.
	cn, err := s.m.NewConsumer([]string{"test2"}, NewConsumerOptions())
	c.Assert(err, IsNil)
	defer cn.Terminate(true)

	// We expect the two partitions to be reclaimed with a simple heartbeat
	// and no claim message sent
	c.Assert(s.m.waitForRsteps(7), Equals, 7)
	c.Assert(len(cn.claims[cn.defaultTopic()]), Equals, 2)
	cn.lock.RLock()
	cl0, cl1 := cn.claims[cn.defaultTopic()][0], cn.claims[cn.defaultTopic()][1]
	cn.lock.RUnlock()
	c.Assert(cl0.offsets.Current, Equals, int64(2))
	c.Assert(cl1.offsets.Current, Equals, int64(0))

	// There should be four messages left, but they can come in any order depending
	// on how things get scheduled. Let's get them all and sort and verify. This
	// does indicate we've double-consumed, but that's expected in this particular
	// failure scenario.
	var msgs []string
	for i := 0; i < 4; i++ {
		msgs = append(msgs, string(cn.consumeOne().Value))
	}
	sort.Strings(msgs)
	c.Assert(msgs, DeepEquals, []string{"m3", "p1", "p2", "p3"})
}

func (s *ConsumerSuite) TestMaximumClaims(c *C) {
	// Test the MaximumClaims option.
	s.cn.options.MaximumClaims = 2
	c.Assert(s.cn.isClaimLimitReached(), Equals, false)
	c.Assert(s.cn.getNumActiveClaims(), Equals, 0)
	s.cn.claimPartitions()
	c.Assert(s.cn.isClaimLimitReached(), Equals, false)
	c.Assert(s.cn.getNumActiveClaims(), Equals, 1)
	s.cn.claimPartitions()
	c.Assert(s.cn.isClaimLimitReached(), Equals, true)
	c.Assert(s.cn.getNumActiveClaims(), Equals, 2)
	s.cn.claimPartitions()
	c.Assert(s.cn.isClaimLimitReached(), Equals, true)
	c.Assert(s.cn.getNumActiveClaims(), Equals, 2)
}

func (s *ConsumerSuite) TestMaximumGreedyClaims(c *C) {
	// Test the MaximumClaims option combined with GreedyClaims.
	s.cn.options.MaximumClaims = 2
	s.cn.options.GreedyClaims = true
	c.Assert(s.cn.isClaimLimitReached(), Equals, false)
	c.Assert(s.cn.getNumActiveClaims(), Equals, 0)
	s.cn.claimPartitions()
	c.Assert(s.cn.isClaimLimitReached(), Equals, true)
	c.Assert(s.cn.getNumActiveClaims(), Equals, 2)
	s.cn.claimPartitions()
	c.Assert(s.cn.isClaimLimitReached(), Equals, true)
	c.Assert(s.cn.getNumActiveClaims(), Equals, 2)
}
