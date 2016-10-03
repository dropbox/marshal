package marshal

import (
	. "gopkg.in/check.v1"

	"github.com/dropbox/kafka/kafkatest"
)

var _ = Suite(&ClusterSuite{})

type ClusterSuite struct {
	s  *kafkatest.Server
	m  *Marshaler
	m2 *Marshaler
}

func (s *ClusterSuite) SetUpTest(c *C) {
	ResetTestLogger(c)

	s.s = StartServer()

	var err error
	s.m, err = NewMarshaler("cl", "gr", []string{s.s.Addr()})
	c.Assert(err, IsNil)
	c.Assert(s.m, NotNil)

	s.m2, err = NewMarshaler("cl", "gr2", []string{s.s.Addr()})
	c.Assert(err, IsNil)
	c.Assert(s.m2, NotNil)
}

func (s *ClusterSuite) TearDownTest(c *C) {
	s.m.Terminate()
	s.s.Close()
}

func (s *ClusterSuite) TestGetTopicState(c *C) {
	// Always works
	c.Assert(s.m.cluster.getPartitionState("gr", "test2", 0), NotNil)

	// Should error (not claimed)
	topic, err := s.m.getClaimedPartitionState("test2", 0)
	c.Assert(topic, IsNil)
	c.Assert(err, NotNil)

	// Now claim this partition
	c.Assert(s.m.ClaimPartition("test2", 0), Equals, true)
	c.Assert(s.m.cluster.waitForRsteps(1), Equals, 1)

	// getClaimed should now work for our group
	topic, err = s.m.getClaimedPartitionState("test2", 0)
	c.Assert(topic, NotNil)
	c.Assert(err, IsNil)

	// And fail here
	topic, err = s.m2.getClaimedPartitionState("test2", 0)
	c.Assert(topic, IsNil)
	c.Assert(err, NotNil)

	// And fail here (our group, diff partition)
	topic, err = s.m.getClaimedPartitionState("test2", 1)
	c.Assert(topic, IsNil)
	c.Assert(err, NotNil)

	// Release partition now
	c.Assert(s.m.ReleasePartition("test2", 0, 0), IsNil)
	c.Assert(s.m.cluster.waitForRsteps(2), Equals, 2)

	// getClaimed should now fail again for our group
	topic, err = s.m.getClaimedPartitionState("test2", 0)
	c.Assert(topic, IsNil)
	c.Assert(err, NotNil)

	// And fail here
	topic, err = s.m2.getClaimedPartitionState("test2", 0)
	c.Assert(topic, IsNil)
	c.Assert(err, NotNil)

	// And fail here (our group, diff partition)
	topic, err = s.m.getClaimedPartitionState("test2", 1)
	c.Assert(topic, IsNil)
	c.Assert(err, NotNil)
}
