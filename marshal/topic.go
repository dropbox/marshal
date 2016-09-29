/*
 * portal - marshal
 *
 * a library that implements an algorithm for doing consumer coordination within Kafka, rather
 * than using Zookeeper or another external system.
 *
 */

package marshal

import (
	"sync"
	"time"
)

// topicState contains information about a given topic.
type topicState struct {
	// claimPartition is which Marshal topic partition to use for coordination of this topic.
	// Read only, set at initialization time so not protected by the lock.
	claimPartition int

	// This lock also protects the contents of the partitions member.
	lock       *sync.RWMutex
	partitions []PartitionClaim
}

// PrintState causes us to log the state of this topic's claims.
func (ts *topicState) PrintState() {
	ts.lock.RLock()
	defer ts.lock.RUnlock()

	now := time.Now().Unix()
	for partID, claim := range ts.partitions {
		state := "CLMD"
		if !claim.claimed(now) {
			state = "----"
		}
		log.Infof("      * %2d [%s]: GPID %s | CLID %s | LHB %d (%d) | LOF %d | PCL %d",
			partID, state, claim.GroupID, claim.ClientID, claim.LastHeartbeat,
			now-claim.LastHeartbeat, claim.CurrentOffset, len(claim.pendingClaims))
	}
}

// PartitionOffsets is a record of offsets for a given partition. Contains information
// combined from Kafka and our current state.
//
// A Kafka partition consists of N messages with offsets. In the basic case, you
// can think of an offset like an array index. With log compaction and other trickery
// it acts more like a sparse array, but it's a close enough metaphor.
//
// We keep track of four values for offsets:
//
//    offsets       1     2     3     7     9    10    11
//   partition  [ msg1, msg2, msg3, msg4, msg5, msg6, msg7, ... ]
//                 ^                  ^                      ^
//                 \- Earliest        |                      |
//                                    \- Current          Latest
//
// In this example, Earliest is 1 which is the "oldest" offset within the
// partition. At any given time this offset might become invalid if a log rolls
// so we might update it.
//
// Current is 7, which is the offset of the NEXT message i.e. this message
// has not been consumed yet.
//
// Latest is 12, which is the offset that Kafka will assign to the message
// that next gets committed to the partition. This offset does not yet exist,
// and might never.
//
// Committed is the value recorded in Kafka's committed offsets system.
type PartitionOffsets struct {
	Current   int64
	Earliest  int64
	Latest    int64
	Committed int64
}

// PartitionClaim contains claim information about a given partition.
type PartitionClaim struct {
	InstanceID    string
	ClientID      string
	GroupID       string
	LastRelease   int64
	LastHeartbeat int64
	CurrentOffset int64

	// Used internally when someone is waiting on this partition to be claimed.
	pendingClaims []chan struct{}
}

// checkOwnership compares the ClientID/GroupID (and optionally InstanceID) of a given
// claim to a given message and returns whether or not they match.
func (p *PartitionClaim) checkOwnership(msg message, checkInstanceID bool) bool {
	iid, cid, gid := msg.Ownership()
	if p.ClientID != cid || p.GroupID != gid {
		return false
	}
	return !checkInstanceID || p.InstanceID == iid
}

// claimed returns a boolean indicating whether or not this structure is indicating a
// still valid claim. Validity is based on the delta between NOW and lastHeartbeat:
//
// delta = 0 .. HeartbeatInterval: claim good.
//         HeartbeatInterval .. 2*HeartbeatInterval-1: claim good.
//         >2xHeartbeatInterval: claim invalid.
//
// This means that the worst case for a "dead consumer" that has failed to heartbeat
// is that a partition will be idle for twice the heartbeat interval.
func (p *PartitionClaim) claimed(ts int64) bool {
	// If lastHeartbeat is 0, then the partition is unclaimed
	if p.LastHeartbeat == 0 {
		return false
	}

	// We believe we have claim information, but let's analyze it to determine whether or
	// not the claim is valid. Of course this assumes that our time and the remote's time
	// are roughly in sync.
	now := ts
	if ts == 0 {
		now = time.Now().Unix()
	}

	delta := now - p.LastHeartbeat
	switch {
	case 0 <= delta && delta <= HeartbeatInterval:
		// Fresh claim - all good
		return true
	case HeartbeatInterval < delta && delta < 2*HeartbeatInterval:
		// Aging claim - missed/delayed heartbeat, but still in tolerance
		return true
	default:
		// Stale claim - no longer valid
		return false
	}
}

// Claimed returns whether or not the PartitionClaim indicates a valid (as of this
// invocation) claim.
func (p *PartitionClaim) Claimed() bool {
	return p.claimed(0)
}
