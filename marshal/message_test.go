package marshal

import . "gopkg.in/check.v1"

var _ = Suite(&MessageSuite{})

type MessageSuite struct{}

func (s *MessageSuite) SetUpTest(c *C) {
	ResetTestLogger(c)
}

func (s *MessageSuite) TestMessageEncode(c *C) {
	base := msgBase{
		Version:    4,
		Time:       2,
		InstanceID: "ii",
		ClientID:   "cl",
		GroupID:    "gr",
		Topic:      "t",
		PartID:     3,
	}
	c.Assert(base.Encode(), Equals, "4/2/ii/cl/gr/t/3")

	hb := msgHeartbeat{
		msgBase:       base,
		CurrentOffset: 5,
	}
	c.Assert(hb.Encode(), Equals, "Heartbeat/4/2/ii/cl/gr/t/3/5")

	cp := msgClaimingPartition{
		msgBase: base,
	}
	c.Assert(cp.Encode(), Equals, "ClaimingPartition/4/2/ii/cl/gr/t/3")

	rp := msgReleasingPartition{
		msgBase:       base,
		CurrentOffset: 7,
	}
	c.Assert(rp.Encode(), Equals, "ReleasingPartition/4/2/ii/cl/gr/t/3/7")

	cm := msgClaimingMessages{
		msgBase:               base,
		ProposedCurrentOffset: 9,
	}
	c.Assert(cm.Encode(), Equals, "ClaimingMessages/4/2/ii/cl/gr/t/3/9")

	rgBase := base
	rgBase.Topic = ""
	rgBase.PartID = 0
	rg := msgReleaseGroup{
		// Base message GroupID identifies which consumer group to release.
		msgBase:       rgBase,
		MsgExpireTime: 12,
	}
	c.Assert(rg.Encode(), Equals, "ReleaseGroup/4/2/ii/cl/gr//0/12")
}

func (s *MessageSuite) TestMessageDecode(c *C) {
	msg, err := decode([]byte("banana"))
	c.Assert(msg, IsNil)
	c.Assert(err, NotNil)

	msg, err = decode([]byte("Heartbeat/4/2/ii/cl/gr/t/1/2"))
	c.Assert(msg, NotNil)
	c.Assert(err, IsNil)

	mhb, ok := msg.(*msgHeartbeat)
	if !ok || msg.Type() != msgTypeHeartbeat || mhb.ClientID != "cl" || mhb.GroupID != "gr" ||
		mhb.Topic != "t" || mhb.PartID != 1 || mhb.CurrentOffset != 2 || mhb.Time != 2 ||
		mhb.Version != 4 {
		c.Error("Heartbeat message contents invalid")
	}

	msg, err = decode([]byte("ClaimingPartition/4/2/ii/cl/gr/t/1"))
	if msg == nil || err != nil {
		c.Error("Expected msg, got error", err)
	}
	mcp, ok := msg.(*msgClaimingPartition)
	if !ok || msg.Type() != msgTypeClaimingPartition || mcp.ClientID != "cl" ||
		mcp.GroupID != "gr" || mcp.Topic != "t" || mcp.PartID != 1 || mcp.Time != 2 ||
		mcp.Version != 4 {
		c.Error("ClaimingPartition message contents invalid")
	}

	msg, err = decode([]byte("ReleasingPartition/4/2/ii/cl/gr/t/1/9"))
	if msg == nil || err != nil {
		c.Error("Expected msg, got error", err)
	}
	mrp, ok := msg.(*msgReleasingPartition)
	if !ok || msg.Type() != msgTypeReleasingPartition || mrp.ClientID != "cl" ||
		mrp.GroupID != "gr" || mrp.Topic != "t" || mrp.PartID != 1 || mrp.Time != 2 ||
		mrp.CurrentOffset != 9 || mhb.Version != 4 {
		c.Error("ReleasingPartition message contents invalid")
	}

	msg, err = decode([]byte("ClaimingMessages/4/2/ii/cl/gr/t/1/2"))
	if msg == nil || err != nil {
		c.Error("Expected msg, got error", err)
	}
	mcm, ok := msg.(*msgClaimingMessages)
	if !ok || msg.Type() != msgTypeClaimingMessages || mcm.ClientID != "cl" || mcm.GroupID != "gr" ||
		mcm.Topic != "t" || mcm.PartID != 1 || mcm.ProposedCurrentOffset != 2 || mcm.Time != 2 ||
		mhb.Version != 4 {
		c.Error("ClaimingMessages message contents invalid")
	}

	msg, err = decode([]byte("ReleaseGroup/4/2/ii/cl/gr//0/12"))
	if msg == nil || err != nil {
		c.Error("Expected msg, got error", err)
	}
	mrg, ok := msg.(*msgReleaseGroup)
	if !ok || msg.Type() != msgTypeReleaseGroup || mrg.ClientID != "cl" || mrg.GroupID != "gr" ||
		mrg.Topic != "" || mrg.PartID != 0 || mrg.MsgExpireTime != 12 || mrg.Time != 2 ||
		mrg.Version != 4 {
		c.Error("ReleaseGroup message contents invalid")
	}
}
