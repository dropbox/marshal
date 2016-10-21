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

	// Create an Admin that sets a pause duration of 15 seconds.
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
	// Rewind to after the first produced message for one partition.
	rewindOffsets := make(map[string]map[int]int64)
	rewindOffsets["test1"] = make(map[int]int64)
	rewindOffsets["test1"][0] = 0
	// We include a second partition to check the case where multiple heartbeat goroutines are
	// instantiated; for example, we want to ensure that there is no panic on shutdown due to
	// "close on closed channel".
	rewindOffsets["test1"][1] = 0

	// Messages that we will try to reconsume after resetting the offset.
	messages := []string{"aren't", "data", "races", "fun"}
	s.Produce("test1", 0, messages...)

	// Set up a consumer and have it claim the only partition for this topic.
	cns := NewTestConsumer(s.m, []string{"test1"})
	s.m.addNewConsumer(cns)
	defer cns.Terminate(true)

	c.Assert(cns.GetCurrentLoad(), Equals, 0)
	cns.claimPartitions()
	go cns.manageClaims()
	c.Assert(cns.GetCurrentLoad(), Equals, 1)

	for i := 0; i < len(messages); i++ {
		msg := cns.consumeOne()
		c.Assert(msg.Value, DeepEquals, []byte(messages[i]))
	}

	// Flush to commit offsets immediately, so that we can check them.
	c.Assert(cns.Flush(), IsNil)
	offsets, err := s.m.GetPartitionOffsets("test1", 0)
	c.Assert(err, IsNil)
	c.Assert(offsets.Committed, Equals, int64(4))

	// Rewind the consumer group to re-consume.
	err = s.a.SetConsumerGroupPosition(s.m.groupID, rewindOffsets)
	c.Assert(err, IsNil)

	offsets, err = s.m.GetPartitionOffsets("test1", 0)
	c.Assert(err, IsNil)
	c.Assert(offsets.Current, Equals, int64(0))
	c.Assert(offsets.Committed, Equals, int64(0))

	// This is janky, but we'll have to wait until the consumer unpauses itself
	// and picks up the claim once again.
	for s.m.cluster.IsGroupPaused(s.m.GroupID()) {
		log.Infof("Group still paused, sleeping...")
		time.Sleep(1 * time.Second)
	}
	// See if the consumer can consume again.
	for i := 0; i < len(messages); i++ {
		msg := cns.consumeOne()
		c.Assert(msg.Value, DeepEquals, []byte(messages[i]))
		log.Infof("%s, %d [%s:%d]\n", msg.Value, msg.Offset, msg.Topic, msg.Partition)
	}
}
