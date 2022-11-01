package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
)

// Server
type Server struct {
	mu sync.Mutex

	serverId int
	peersIds []int

	raftLogic *RaftNode

	RPCServer *rpc.Server
	listener  net.Listener

	peerClients map[int]*rpc.Client

	ready <-chan interface{}
	quit  chan interface{}
	wg    sync.WaitGroup
}

func NewRPCServer(serverId int, peersIds []int, ready <-chan interface{}) *Server {
	this := new(Server)
	this.serverId = serverId
	this.peersIds = peersIds
	this.peerClients = make(map[int]*rpc.Client)

	this.ready = ready
	this.quit = make(chan interface{})

	return this
}

func (this *Server) Serve() {
	this.mu.Lock()
	this.raftLogic = NewRaftNode(this.serverId, this.peersIds, this, this.ready)

	// Create a new RPC server
	this.RPCServer = rpc.NewServer()
	this.RPCServer.RegisterName("RaftNode", this)

	var err error
	if this.listener, err = net.Listen("tcp", ":0"); err != nil {
		log.Fatal(err)
	}

	log.Printf("[%v] listening at %v", this.serverId, this.listener.Addr())
	this.mu.Unlock()

	this.wg.Add(1)
	go func() {
		defer this.wg.Done()

		for {
			conn, err := this.listener.Accept()
			if err != nil {
				select {
				case <-this.quit:
					return
				default:
					log.Fatal("accept error:", err)
				}
			}
			this.wg.Add(1)
			go func() {
				this.RPCServer.ServeConn(conn)
				this.wg.Done()
			}()
		}
	}()

}

func (this *Server) GetCurrentAddress() net.Addr {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.listener.Addr()
}

func (this *Server) Call(id int, serviceMethod string, args interface{}, reply interface{}) error {
	this.mu.Lock()
	peer := this.peerClients[id]
	this.mu.Unlock()

	if peer == nil {
		return fmt.Errorf("call client %d after it'this closed", id)
	} else {
		return peer.Call(serviceMethod, args, reply)
	}
}

/* To actually add a delay for each request */

func (this *Server) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	if rand.Intn(10) == 7 {
		return fmt.Errorf("RPC failed")
	}
	sleepMs(20 + rand.Intn(500))
	return this.raftLogic.HandleRequestVote(args, reply)
}

func (this *Server) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	if rand.Intn(10) == 7 {
		return fmt.Errorf("RPC failed")
	}
	sleepMs(20 + rand.Intn(500))
	return this.raftLogic.HandleAppendEntries(args, reply)
}

/* Functions that facilitate peer to peer connection/disconnection */

func (this *Server) ConnectToPeer(peerId int, addr net.Addr) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.peerClients[peerId] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}
		this.peerClients[peerId] = client
	}
	return nil
}

func (this *Server) DisconnectPeer(peerId int) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.peerClients[peerId] != nil {
		err := this.peerClients[peerId].Close()
		this.peerClients[peerId] = nil
		return err
	}
	return nil
}

func (this *Server) DisconnectAll() {
	this.mu.Lock()
	defer this.mu.Unlock()
	for id := range this.peerClients {
		if this.peerClients[id] != nil {
			this.peerClients[id].Close()
			this.peerClients[id] = nil
		}
	}
}

func (this *Server) Shutdown() {
	this.raftLogic.KillNode()
	close(this.quit)
	this.listener.Close()
	this.wg.Wait()
}