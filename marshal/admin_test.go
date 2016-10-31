package marshal

import (
	"time"

	. "gopkg.in/check.v1"

	"github.com/dropbox/kafka/kafkatest"
	"github.com/dropbox/kafka/proto"
)

var _ = Suite(&AdminSuite{})

type AdminSuite struct {
	c      *C
	s      *kafkatest.Server
	m      *Marshaler
	mAdmin *Marshaler
	a      Admin
}

func (s *AdminSuite) SetUpTest(c *C) {
	ResetTestLogger(c)

	s.c = c
	s.s = StartServer()

	// Use Dial to create multiple Marshalers - one for the consumer, one for the admin.
	brokers := []string{s.s.Addr()}
	cluster, err := Dial("automatic", brokers, NewMarshalOptions())
	c.Assert(err, IsNil)
	s.m, err = cluster.NewMarshaler("cl-w-admin", "gr-w-admin")
	c.Assert(err, IsNil)
	s.mAdmin, err = cluster.NewMarshaler("cl-admin", "gr-w-admin")
	c.Assert(err, IsNil)

	// Create an Admin that sets a pause duration
	s.a, err = s.mAdmin.NewAdmin("gr-w-admin", 15*time.Second)
	c.Assert(err, IsNil)
}

func (s *AdminSuite) TearDownTest(c *C) {
	s.m.Terminate()
	s.s.Close()
}

func (s *AdminSuite) Produce(topicName string, partID int, msgs ...string) int64 {
	var protos []*proto.Message
	for _, msg := range msgs {
		protos = append(protos, &proto.Message{Value: []byte(msg)})
	}
	offset, err := s.m.cluster.producer.Produce(topicName, int32(partID), protos...)
	s.c.Assert(err, IsNil)
	return offset
}

// Tests rewinding the position a consumer group reads from.
func (s *AdminSuite) TestRewindConsumer(c *C) {
	// messages[i] is the list of messages that will be produced for partition i.
	messages := [][]string{
		[]string{"aren't", "data", "races", "fun"},
		[]string{"data", "fun", "aren't", "races"},
	}
	s.Produce("test2", 0, messages[0]...)
	s.Produce("test2", 1, messages[1]...)

	// We rewind to the beginning for partition 0, and to the middle for partition 1.
	midIndex := 2
	rewindOffsets := make(map[string]map[int]int64)
	rewindOffsets["test2"] = make(map[int]int64)
	rewindOffsets["test2"][0] = 0
	rewindOffsets["test2"][1] = int64(midIndex)

	// Set up a consumer and have it claim the partitions for this topic.
	cns := NewTestConsumer(s.m, []string{"test2"})

	// Turn on GreedyClaims so that the consumer claims both partitions for the "test2" topic.
	cns.options.GreedyClaims = true
	s.m.addNewConsumer(cns)
	defer cns.Terminate(true)

	// Ensure that both partitions for the "test2" topic are successfully claimed.
	c.Assert(cns.GetCurrentLoad(), Equals, 0)
	cns.claimPartitions()
	go cns.manageClaims()
	c.Assert(cns.GetCurrentLoad(), Equals, 2)

	// Ensure that all produced messages are successfully consumed.
	consumed := []int{0, 0}
	for i := 0; i < len(messages[0])+len(messages[1]); i++ {
		msg := cns.consumeOne()
		c.Assert(msg.Value, DeepEquals, []byte(messages[msg.Partition][consumed[msg.Partition]]))
		consumed[msg.Partition]++
		log.Infof("%s, %d [%s:%d]\n", msg.Value, msg.Offset, msg.Topic, msg.Partition)
	}

	// Flush to commit offsets immediately, so that we can check them.
	c.Assert(cns.Flush(), IsNil)
	c.Assert(s.m.cluster.waitForRsteps(6), Equals, 6)

	// Both partitions should have been fully consumed and committed.
	offsets, err := s.m.GetPartitionOffsets("test2", 0)
	c.Assert(err, IsNil)
	c.Assert(offsets.Current, Equals, int64(len(messages[0])))
	c.Assert(offsets.Committed, Equals, int64(len(messages[0])))

	offsets, err = s.m.GetPartitionOffsets("test2", 1)
	c.Assert(err, IsNil)
	c.Assert(offsets.Current, Equals, int64(len(messages[1])))
	c.Assert(offsets.Committed, Equals, int64(len(messages[1])))

	// Rewind the consumer group to re-consume.
	err = s.a.SetConsumerGroupPosition(s.m.groupID, rewindOffsets)
	c.Assert(err, IsNil)

	// This is janky, but we'll have to wait until the consumer unpauses itself
	// and picks up the claim once again.
	for s.m.cluster.IsGroupPaused(s.m.GroupID()) {
		log.Infof("Group still paused, sleeping...")
		time.Sleep(1 * time.Second)
	}

	// Partition 0 should have been reset to the beginning.
	offsets, err = s.m.GetPartitionOffsets("test2", 0)
	c.Assert(err, IsNil)
	c.Assert(offsets.Current, Equals, int64(0))
	c.Assert(offsets.Committed, Equals, int64(0))

	// Partition 1 should have been reset to the middle.
	offsets, err = s.m.GetPartitionOffsets("test2", 1)
	c.Assert(err, IsNil)
	c.Assert(offsets.Current, Equals, int64(midIndex))
	c.Assert(offsets.Committed, Equals, int64(midIndex))

	// See if the consumer can consume again.  Note that for partition 1, consumption should start
	// from the middle.
	consumed = []int{0, midIndex}
	for i := 0; i < len(messages[0])+len(messages[1][midIndex:]); i++ {
		msg := cns.consumeOne()
		c.Assert(msg.Value, DeepEquals, []byte(messages[msg.Partition][consumed[msg.Partition]]))
		consumed[msg.Partition]++
		log.Infof("%s, %d [%s:%d]\n", msg.Value, msg.Offset, msg.Topic, msg.Partition)
	}
}
