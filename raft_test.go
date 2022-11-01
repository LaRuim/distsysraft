package raft

import (
	"testing"
)

func Test1a(t *testing.T) { // Simple Leader Election

	ca := NewCluster(t, 5)
	defer ca.Shutdown()

	sleepMs(3000) // Wait for a leader to be elected

	firstLeaderId, _ := ca.getClusterLeader()
	ca.DisconnectPeer(firstLeaderId)

	secondLeaderId, _ := ca.getClusterLeader()
	ca.DisconnectPeer(secondLeaderId)

	thirdLeaderId, _ := ca.getClusterLeader()
	ca.DisconnectPeer(thirdLeaderId)

	sleepMs(3000)

	// Fails, no leader present
	ca.getClusterLeader()
	sleepMs(3000)

}

func Test1b(t *testing.T) { // Slightly more Complicated Leader Election

	ca := NewCluster(t, 5)
	defer ca.Shutdown()

	firstLeaderId, _ := ca.getClusterLeader()

	ca.DisconnectPeer(firstLeaderId)

	secondLeaderId, _ := ca.getClusterLeader()

	ca.DisconnectPeer(secondLeaderId)

	thirdLeaderId, _ := ca.getClusterLeader()

	ca.DisconnectPeer(thirdLeaderId)

	sleepMs(3000)
	ca.ReconnectPeer(firstLeaderId)
	ca.ReconnectPeer(secondLeaderId)

	sleepMs(6000)
	// Fails, no leader present
	ca.getClusterLeader()

	sleepMs(6000)

}

func Test2(t *testing.T) {
	/* Replication failure scenario: Leader drops after committing, comes back later*/

	ca := NewCluster(t, 5)
	defer ca.Shutdown()

	// ReceiveClientCommand a couple of values to a fully connected nodes.
	origLeaderId, _ := ca.getClusterLeader()
	ca.SubmitClientCommand(origLeaderId, "Set X = 5")
	ca.SubmitClientCommand(origLeaderId, "Set X = 1000")

	sleepMs(3000)

	// Leader disconnected...
	ca.DisconnectPeer(origLeaderId)

	// ReceiveClientCommand 7 to original leader, even though it's disconnected. Should not reflect.
	ca.SubmitClientCommand(origLeaderId, "Set X = X-5")

	newLeaderId, _ := ca.getClusterLeader()

	// ReceiveClientCommand 8.. to new leader.
	ca.SubmitClientCommand(newLeaderId, "Set X = X+10")
	ca.SubmitClientCommand(newLeaderId, "Set X = X+1")
	ca.SubmitClientCommand(newLeaderId, "Set Y = 5")
	ca.SubmitClientCommand(newLeaderId, "Set Y = X+Y")
	ca.SubmitClientCommand(newLeaderId, "Set Y = Y+3")
	ca.SubmitClientCommand(newLeaderId, "Set Z = -1")
	sleepMs(3000)

	// ReceiveClientCommand 9 and check it's fully committed.
	ca.SubmitClientCommand(newLeaderId, "Set Z = 3")
	sleepMs(3000)

	ca.ReconnectPeer(origLeaderId)
	sleepMs(15000)
}

func Test3(t *testing.T) {
	/* Log Replication failure scenario:
	Leader drops without committing, comes back later */

	ca := NewCluster(t, 5)
	defer ca.Shutdown()

	/* Original leader is queried a couple of times and is made to disconnect
	without the recieving the ack for the queries across followers
	i.e without applying the queries yet. */

	origLeaderId, _ := ca.getClusterLeader()

	ca.SubmitClientCommand(origLeaderId, "Set X=3")
	ca.SubmitClientCommand(origLeaderId, "Set X=5")
	ca.SubmitClientCommand(origLeaderId, "Set X=6")
	ca.SubmitClientCommand(origLeaderId, "Set X=X+2")
	ca.SubmitClientCommand(origLeaderId, "Set X=0")

	ca.DisconnectPeer(origLeaderId)

	// New Leader is elected and queried the same number of times
	// Here the changes are to be applied across all nodes in the harness
	newLeaderId, _ := ca.getClusterLeader()

	ca.SubmitClientCommand(newLeaderId, "Set X = 49")
	ca.SubmitClientCommand(newLeaderId, "Set Y = 62")
	ca.SubmitClientCommand(newLeaderId, "Set X = 73")
	ca.SubmitClientCommand(newLeaderId, "Set X = 433")
	ca.SubmitClientCommand(newLeaderId, "Set Y = 510")

	sleepMs(6000)

	/* Here we want to see how the OLD leader will behave on finding the queries were not
	applied and new queries have already been applied and taken place
	Expected behavior:
		- Leader will apply only the queries that have aleady been applied with a majority vote
		- Leader will discard its previous staged queries that did not get acked across the nodes
		- The File IO will NOT have ANY of the received BUT NOT APPLIED queries and ONLY HAVE APPLIED QUERIES
	*/

	ca.ReconnectPeer(origLeaderId)

	sleepMs(3000)
}

func Test4(t *testing.T) {
	/* Log Replication Scenario: Leader commits, peer drops;
	Then Leader drops, then peer comes back;
	must not be elected; New leader gets commands, commits;
	old leader returns, gets up to date. */

	ca := NewCluster(t, 5)
	defer ca.Shutdown()

	origLeaderId, _ := ca.getClusterLeader()
	ca.SubmitClientCommand(origLeaderId, "Set X = 5")
	ca.SubmitClientCommand(origLeaderId, "Set X = X+1")
	ca.SubmitClientCommand(origLeaderId, "Set X = X+2")

	sleepMs(2000)
	// Disconnect peer that isn't leader

	ca.DisconnectPeer(5 - origLeaderId)

	ca.SubmitClientCommand(origLeaderId, "Set Y = 121")
	ca.SubmitClientCommand(origLeaderId, "Set Y = 800")

	sleepMs(2000)

	ca.DisconnectPeer(origLeaderId)
	ca.ReconnectPeer(5 - origLeaderId)

	newLeaderId, _ := ca.getClusterLeader()
	ca.SubmitClientCommand(newLeaderId, "Set Y = 999")
	ca.SubmitClientCommand(newLeaderId, "Set Y = 1200")
	sleepMs(2000)

	// Disconnected Peer must now get the missed entries also
	ca.ReconnectPeer(origLeaderId)

	sleepMs(2000)
	//Old leader becomes follower and gets all the Log Entries

}
