package raft

import (
	"testing"
)

func Test1a(t *testing.T) { // Simple Leader Election

	cluster := NewCluster(t, 5)
	defer cluster.Shutdown()

	sleepMs(3000) // Wait for a leader to be elected

	firstLeaderId := cluster.getClusterLeader()
	cluster.DisconnectPeer(firstLeaderId)

	secondLeaderId := cluster.getClusterLeader()
	cluster.DisconnectPeer(secondLeaderId)

	thirdLeaderId := cluster.getClusterLeader()
	cluster.DisconnectPeer(thirdLeaderId)

	sleepMs(3000)

	// Fails, no leader present
	cluster.getClusterLeader()
	sleepMs(3000)

}

func Test1b(t *testing.T) { // Slightly more Complicated Leader Election

	cluster := NewCluster(t, 5)
	defer cluster.Shutdown()

	firstLeaderId := cluster.getClusterLeader()

	cluster.DisconnectPeer(firstLeaderId)

	secondLeaderId := cluster.getClusterLeader()

	cluster.DisconnectPeer(secondLeaderId)

	thirdLeaderId := cluster.getClusterLeader()

	cluster.DisconnectPeer(thirdLeaderId)

	sleepMs(3000)
	cluster.ReconnectPeer(firstLeaderId)
	cluster.ReconnectPeer(secondLeaderId)

	sleepMs(6000)
	// Fails, no leader present
	cluster.getClusterLeader()

	sleepMs(6000)

}

func Test2(t *testing.T) {
	/* Replication failure scenario: Leader drops after committing, comes back later*/

	cluster := NewCluster(t, 5)
	defer cluster.Shutdown()

	// ReceiveClientCommand a couple of values to a fully connected nodes.
	origLeaderId := cluster.getClusterLeader()
	cluster.SubmitClientCommand(origLeaderId, "Set X = 5")
	cluster.SubmitClientCommand(origLeaderId, "Set X = 1000")

	sleepMs(3000)

	// Leader disconnected...
	cluster.DisconnectPeer(origLeaderId)

	// ReceiveClientCommand 7 to original leader, even though it's disconnected. Should not reflect.
	cluster.SubmitClientCommand(origLeaderId, "Set X = X-5")

	newLeaderId := cluster.getClusterLeader()

	// ReceiveClientCommand 8.. to new leader.
	cluster.SubmitClientCommand(newLeaderId, "Set X = X+10")
	cluster.SubmitClientCommand(newLeaderId, "Set X = X+1")
	cluster.SubmitClientCommand(newLeaderId, "Set Y = 5")
	cluster.SubmitClientCommand(newLeaderId, "Set Y = X+Y")
	cluster.SubmitClientCommand(newLeaderId, "Set Y = Y+3")
	cluster.SubmitClientCommand(newLeaderId, "Set Z = -1")
	sleepMs(3000)

	// ReceiveClientCommand 9 and check it's fully committed.
	cluster.SubmitClientCommand(newLeaderId, "Set Z = 3")
	sleepMs(3000)

	cluster.ReconnectPeer(origLeaderId)
	sleepMs(15000)
}

func Test3(t *testing.T) {
	/* Log Replication failure scenario:
	Leader drops without committing, comes back later */

	cluster := NewCluster(t, 5)
	defer cluster.Shutdown()

	/* Original leader is queried a couple of times and is made to disconnect
	without the recieving the ack for the queries across followers
	i.e without applying the queries yet. */

	origLeaderId := cluster.getClusterLeader()

	cluster.SubmitClientCommand(origLeaderId, "Set X=3")
	cluster.SubmitClientCommand(origLeaderId, "Set X=5")
	cluster.SubmitClientCommand(origLeaderId, "Set X=6")
	cluster.SubmitClientCommand(origLeaderId, "Set X=X+2")
	cluster.SubmitClientCommand(origLeaderId, "Set X=0")

	cluster.DisconnectPeer(origLeaderId)

	// New Leader is elected and queried the same number of times
	// Here the changes are to be applied across all nodes in the harness
	newLeaderId := cluster.getClusterLeader()

	cluster.SubmitClientCommand(newLeaderId, "Set X = 49")
	cluster.SubmitClientCommand(newLeaderId, "Set Y = 62")
	cluster.SubmitClientCommand(newLeaderId, "Set X = 73")
	cluster.SubmitClientCommand(newLeaderId, "Set X = 433")
	cluster.SubmitClientCommand(newLeaderId, "Set Y = 510")

	sleepMs(6000)

	/* Here we want to see how the OLD leader will behave on finding the queries were not
	applied and new queries have already been applied and taken place
	Expected behavior:
		- Leader will apply only the queries that have aleady been applied with a majority vote
		- Leader will discard its previous staged queries that did not get acked across the nodes
		- The File IO will NOT have ANY of the received BUT NOT APPLIED queries and ONLY HAVE APPLIED QUERIES
	*/

	cluster.ReconnectPeer(origLeaderId)

	sleepMs(3000)
}

func Test4(t *testing.T) {
	/* Log Replication Scenario: Leader commits, peer drops;
	Then Leader drops, then peer comes back;
	must not be elected; New leader gets commands, commits;
	old leader returns, gets up to date. */

	cluster := NewCluster(t, 5)
	defer cluster.Shutdown()

	origLeaderId := cluster.getClusterLeader()
	cluster.SubmitClientCommand(origLeaderId, "Set X = 5")
	cluster.SubmitClientCommand(origLeaderId, "Set X = X+1")
	cluster.SubmitClientCommand(origLeaderId, "Set X = X+2")

	sleepMs(2000)
	// Disconnect peer that isn't leader

	cluster.DisconnectPeer(5 - origLeaderId)

	cluster.SubmitClientCommand(origLeaderId, "Set Y = 121")
	cluster.SubmitClientCommand(origLeaderId, "Set Y = 800")

	sleepMs(2000)

	cluster.DisconnectPeer(origLeaderId)
	cluster.ReconnectPeer(5 - origLeaderId)

	newLeaderId := cluster.getClusterLeader()
	cluster.SubmitClientCommand(newLeaderId, "Set Y = 999")
	cluster.SubmitClientCommand(newLeaderId, "Set Y = 1200")
	sleepMs(2000)

	// Disconnected Peer must now get the missed entries also
	cluster.ReconnectPeer(origLeaderId)

	sleepMs(2000)
	//Old leader becomes follower and gets all the Log Entries

}
