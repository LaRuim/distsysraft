package raft

import "time"

// startLeader switches this into a leader state and begins process of heartbeats.
func (this *RaftNode) startLeader() {
	this.state = "Leader"

	for _, peerId := range this.peersIds {
		this.nextIndex[peerId] = len(this.log)
		this.matchIndex[peerId] = -1
	}
	this.write_log("became Leader; term=%d, nextIndex=%v, matchIndex=%v; log=%v", this.currentTerm, this.nextIndex, this.matchIndex, this.log)

	go func() {
		ticker := time.NewTicker(1000 * time.Millisecond)
		defer ticker.Stop()

		// Send periodic heartbeats, as long as still leader.
		for {
			this.broadcastHeartbeats()
			<-ticker.C

			this.mu.Lock()
			if this.state != "Leader" {
				this.mu.Unlock()
				return
			}
			this.mu.Unlock()
		}
	}()
}

// broadcastHeartbeats sends a round of heartbeats to all peers, collects their replies and adjusts this's state.
func (this *RaftNode) broadcastHeartbeats() {
	this.mu.Lock()

	if this.state != "Leader" {
		this.mu.Unlock()
		return
	}
	termWhenHeartbeatSent := this.currentTerm

	this.mu.Unlock()

	for _, peerId := range this.peersIds {

		go func(peerId int) {
			this.mu.Lock()

			ni := this.nextIndex[peerId]
			prevLogIndex := ni - 1
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = this.log[prevLogIndex].Term
			}
			entries := this.log[ni:]

			var aeType string
			if len(entries) > 0 {
				aeType = "AppendEntries"
			} else {
				aeType = "Heartbeat"
			}

			args := AppendEntriesArgs{
				Term:         termWhenHeartbeatSent,
				LeaderId:     this.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: this.commitIndex,
			}

			this.mu.Unlock()
			if (aeType == "Heartbeat" && HeartbeatLogs) || aeType == "AppendEntries" {
				this.write_log("sending %s to %v: ni=%d, args=%+v", aeType, peerId, ni, args)
			}

			var reply AppendEntriesReply
			if err := this.server.SendRPCCallTo(peerId, "RaftNode.AppendEntries", args, &reply); err == nil {
				this.mu.Lock()
				defer this.mu.Unlock()

				if reply.Term > this.currentTerm {
					this.becomeFollower(reply.Term)
					return
				}

				if this.state == "Leader" && termWhenHeartbeatSent == reply.Term {
					if reply.Success {
						this.nextIndex[peerId] = ni + len(entries)
						this.matchIndex[peerId] = this.nextIndex[peerId] - 1
						if (aeType == "Heartbeat" && HeartbeatLogs) || aeType == "AppendEntries" {
							this.write_log("%s reply from %d success: nextIndex := %v, matchIndex := %v", aeType, peerId, this.nextIndex, this.matchIndex)
						}
						oldCommitIndex := this.commitIndex
						for i := this.commitIndex + 1; i < len(this.log); i++ {
							if this.log[i].Term == this.currentTerm {
								matchCount := 1
								for _, peerId := range this.peersIds {
									if this.matchIndex[peerId] >= i {
										matchCount++
									}
								}
								if matchCount*2 > len(this.peersIds)+1 {
									this.commitIndex = i
								}
							}
						}
						if this.commitIndex != oldCommitIndex {
							this.write_log("leader sets commitIndex := %d", this.commitIndex)
							this.notifyToApplyCommit <- 1
						}
					} else {
						this.nextIndex[peerId] = ni - 1
						if (aeType == "Heartbeat" && HeartbeatLogs) || aeType == "AppendEntries" {
							this.write_log("%s reply from %d was failure; Hence, decrementing nextIndex", aeType, peerId)
						}
					}
				}
			}
		}(peerId)
	}
}
