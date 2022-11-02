package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

const HeartbeatLogs = false
const VoteRequestLogs = true

type LogEntry struct {
	Command interface{}
	Term    int
}

// Main Raft Data Structure
type RaftNode struct {
	mu sync.Mutex

	id       int
	peersIds []int

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile Raft state on leaders
	nextIndex  map[int]int
	matchIndex map[int]int

	// Utility States
	state                        string
	lastElectionTimerStartedTime time.Time
	notifyToApplyCommit          chan int
	LOG_ENTRIES                  bool
	filePath                     string

	// Networking Component
	server *Server
}

// Constructor for RaftNodes
func NewRaftNode(id int, peersIds []int, server *Server, ready <-chan interface{}) *RaftNode {
	this := new(RaftNode)

	this.server = server
	this.notifyToApplyCommit = make(chan int, 16)

	this.id = id
	this.peersIds = peersIds

	this.votedFor = -1
	this.currentTerm = 0

	this.commitIndex = -1
	this.lastApplied = -1

	this.nextIndex = make(map[int]int)
	this.matchIndex = make(map[int]int)

	this.state = "Follower"

	this.LOG_ENTRIES = true

	this.filePath = "NodeLogs/" + strconv.Itoa(this.id)
	f, _ := os.Create(this.filePath)
	f.Close()

	go func() {
		// Signalled when all servers are up and running, ready to receive RPCs
		<-ready

		this.mu.Lock()
		this.lastElectionTimerStartedTime = time.Now()
		this.mu.Unlock()

		this.startElectionTimer()
	}()

	go this.applyCommitedLogEntries() // Fire off forever running watcher to apply any committed entries

	return this
}

/* startElectionTimer implements an election timer. It should be launched whenever
we want to start a timer towards becoming a candidate in a new election.
This function runs as a go routine */
func (this *RaftNode) startElectionTimer() {
	timeoutDuration := time.Duration(3000+rand.Intn(3000)) * time.Millisecond
	this.mu.Lock()
	termStarted := this.currentTerm
	this.mu.Unlock()
	this.write_log("Election timer started: %v, with term=%d", timeoutDuration, termStarted)

	// Keep checking for a resolution
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		this.mu.Lock()

		// if node has become a leader
		if this.state != "Candidate" && this.state != "Follower" {
			this.mu.Unlock()
			return
		}

		// if node received requestVote or appendEntries of a higher term and updated itself
		if termStarted != this.currentTerm {
			this.mu.Unlock()
			return
		}

		// Start an election if we haven't heard from a leader or haven't voted for someone for the duration of the timeout.
		if elapsed := time.Since(this.lastElectionTimerStartedTime); elapsed >= timeoutDuration {
			this.startElection()
			this.mu.Unlock()
			return
		}
		this.mu.Unlock()
	}
}

// startElection starts a new election with this RN as a candidate.
func (this *RaftNode) startElection() {
	this.state = "Candidate"
	this.currentTerm += 1
	termWhenVoteRequested := this.currentTerm
	this.lastElectionTimerStartedTime = time.Now()
	this.votedFor = this.id
	this.write_log("became Candidate with term=%d;", termWhenVoteRequested)

	votesReceived := 1

	// Send RequestVote RPCs to all other servers concurrently.
	for _, peerId := range this.peersIds {
		go func(peerId int) {
			this.mu.Lock()
			var LastLogIndexWhenVoteRequested, LastLogTermWhenVoteRequested int

			if len(this.log) > 0 {
				lastIndex := len(this.log) - 1
				LastLogIndexWhenVoteRequested, LastLogTermWhenVoteRequested = lastIndex, this.log[lastIndex].Term
			} else {
				LastLogIndexWhenVoteRequested, LastLogTermWhenVoteRequested = -1, -1
			}
			this.mu.Unlock()

			args := RequestVoteArgs{
				Term:         termWhenVoteRequested,
				CandidateId:  this.id,
				LastLogIndex: LastLogIndexWhenVoteRequested,
				LastLogTerm:  LastLogTermWhenVoteRequested,
			}

			if VoteRequestLogs {
				this.write_log("sending RequestVote to %d: %+v", peerId, args)
			}

			var reply RequestVoteReply
			if err := this.server.SendRPCCallTo(peerId, "RaftNode.RequestVote", args, &reply); err == nil {
				this.mu.Lock()
				defer this.mu.Unlock()
				if VoteRequestLogs {
					this.write_log("received RequestVoteReply from %d: %+v", peerId, reply)
				}
				if this.state != "Candidate" {
					this.write_log("State changed from Candidate to %s", this.state)
					return
				}

				if reply.Term > termWhenVoteRequested {
					this.becomeFollower(reply.Term)
					return
				} else if reply.Term == termWhenVoteRequested {
					if reply.VoteGranted {
						votesReceived += 1
						if votesReceived*2 > len(this.peersIds)+1 {
							this.write_log("WON THE ELECTION! with %d votes", votesReceived)
							this.startLeader()
							return
						}
					}
				}
			}
		}(peerId)
	}

	// Run another election timer, in case this election is not successful.
	go this.startElectionTimer()
}

// becomeFollower sets a node to be a follower and resets its state.
func (this *RaftNode) becomeFollower(term int) {
	this.write_log("became Follower with term=%d; log=%v", term, this.log)
	this.state = "Follower"
	this.currentTerm = term
	this.votedFor = -1
	this.lastElectionTimerStartedTime = time.Now()

	go this.startElectionTimer()
}

// This function implements the 'application' of a query to the leader
// This is the function that also writes queries accepted by the leader to files
// to observe as output
func (this *RaftNode) applyCommitedLogEntries() {
	for range this.notifyToApplyCommit {
		this.mu.Lock()

		var entriesToApply []LogEntry

		if this.commitIndex > this.lastApplied {
			entriesToApply = this.log[this.lastApplied+1 : this.commitIndex+1]
		}

		f, _ := os.OpenFile(this.filePath, os.O_APPEND|os.O_WRONLY, 0644)

		defer f.Close()

		for i, entry := range entriesToApply {
			strentry := fmt.Sprintf("%s; T:[%d]; I:[%d]", entry.Command, this.currentTerm, this.commitIndex+i)
			f.WriteString(strentry)
			f.WriteString("\n")
		}

		this.lastApplied = this.commitIndex
		this.mu.Unlock()
	}

	this.write_log("applyCommitedLogEntries done")
}

// Kills a RaftNode and sets its state to Dead
func (this *RaftNode) KillNode() {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.state = "Dead"
	this.write_log("becomes Dead")
	close(this.notifyToApplyCommit)
}

// This function logs all messages to the terminal
func (this *RaftNode) write_log(format string, args ...interface{}) {
	if this.LOG_ENTRIES {
		format = fmt.Sprintf("AT NODE %d: ", this.id) + format
		log.Printf(format, args...)
	}
}

// GetNodeState reports the state of this RN.
func (this *RaftNode) GetNodeState() (id int, term int, isLeader bool) {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.id, this.currentTerm, this.state == "Leader"
}
