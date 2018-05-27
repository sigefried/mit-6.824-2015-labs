package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"

import "time"

// for debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
}

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int
type ErrCode int

const (
	ErrOK ErrCode = iota + 1
	ErrReject
	ErrRpc
)

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	instance map[int]*PaxosInst
	done     []int
}

type PaxosInst struct {
	status  Fate
	value   interface{}
	platest int64
	alatest int64
}

type PrepareReq struct {
	Seq   int
	Stamp int64
}

type PrepareRsp struct {
	Code  ErrCode
	Value interface{}
	Stamp int64
}

type AcceptReq struct {
	Seq   int
	Value interface{}
	Stamp int64
}

type AcceptRsp struct {
	Code ErrCode
}

type DecideReq struct {
	Seq   int
	Value interface{}
	// piggyback the done value
	PiggyDoneId    int
	PiggyDoneState int
}

type DecideRsp struct {
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (px *Paxos) PrepareHandler(req *PrepareReq, rsp *PrepareRsp) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	inst, ok := px.instance[req.Seq]
	if !ok {
		inst = &PaxosInst{Pending, nil, req.Stamp, 0}
		px.instance[req.Seq] = inst
		rsp.Code = ErrOK
	} else if req.Stamp > inst.platest { //NOT THE SAME TAMP????
		inst.platest = req.Stamp
		rsp.Code = ErrOK
	} else {
		rsp.Code = ErrReject
	}

	rsp.Value = inst.value
	rsp.Stamp = inst.alatest
	return nil
}

func (px *Paxos) AcceptHandler(req *AcceptReq, rsp *AcceptRsp) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	inst, ok := px.instance[req.Seq]

	if !ok {
		inst = &PaxosInst{Pending, req.Value, req.Stamp, req.Stamp}
		px.instance[req.Seq] = inst
		rsp.Code = ErrOK
	} else if inst.platest <= req.Stamp {
		inst.alatest = req.Stamp
		inst.platest = req.Stamp
		inst.value = req.Value
		rsp.Code = ErrOK
	} else {
		rsp.Code = ErrReject
	}
	return nil
}

func (px *Paxos) DecisionHandler(req *DecideReq, rsp *DecideRsp) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	inst, ok := px.instance[req.Seq]
	//set value
	if !ok {
		inst = &PaxosInst{Decided, req.Value, 0, 0}
		px.instance[req.Seq] = inst
	} else {
		inst.status = Decided
		inst.value = req.Value
	}

	// update done state
	px.done[req.PiggyDoneId] = req.PiggyDoneState
	return nil
}

func (px *Paxos) sendPrepare(seq int, v interface{}) (bool, int64, interface{}) {
	ch := make(chan PrepareRsp, len(px.peers))
	timestamp := time.Now().UnixNano()
	for i, p := range px.peers {
		if i == px.me {
			go func() {
				rsp := PrepareRsp{}
				px.PrepareHandler(&PrepareReq{seq, timestamp}, &rsp)
				ch <- rsp
			}()
		} else {
			go func(host string) {
				rsp := PrepareRsp{}
				if !call(host, "Paxos.PrepareHandler", &PrepareReq{seq, timestamp}, &rsp) {
					rsp.Code = ErrRpc
				}
				ch <- rsp
			}(p)
		}
	}

	// count response
	agreed := 0
	highest := int64(0)
	nv := interface{}(nil)
	for i := 0; i < len(px.peers); i++ {
		rsp := <-ch
		if rsp.Code == ErrOK {
			agreed++
			if rsp.Stamp > highest {
				highest = rsp.Stamp
				nv = rsp.Value
			}
		}
	}

	// if less than n/2 peers agree, failed
	if agreed <= len(px.peers)/2 {
		return false, timestamp, nv
	}

	return true, timestamp, nv
}

func (px *Paxos) sendAccept(seq int, v interface{}, stamp int64) bool {
	ch := make(chan AcceptRsp, len(px.peers))
	for i, p := range px.peers {
		if i == px.me {
			go func() {
				rsp := AcceptRsp{}
				px.AcceptHandler(&AcceptReq{seq, v, stamp}, &rsp)
				ch <- rsp
			}()
		} else {
			go func(host string) {
				rsp := AcceptRsp{}
				if !call(host, "Paxos.AcceptHandler", &AcceptReq{seq, v, stamp}, &rsp) {
					rsp.Code = ErrRpc
				}
				ch <- rsp
			}(p)
		}
	}

	accept := 0
	for i := 0; i < len(px.peers); i++ {
		rsp := <-ch
		if rsp.Code == ErrOK {
			accept++
		}
	}

	if accept <= len(px.peers)/2 {
		return false
	}
	return true
}

func (px *Paxos) sendDecision(seq int, v interface{}) {
	ch := make(chan DecideRsp, len(px.peers))

	for i, p := range px.peers {
		if i == px.me {
			go func() {
				rsp := DecideRsp{}
				px.DecisionHandler(&DecideReq{seq, v, px.me, px.done[px.me]}, &rsp)
				ch <- rsp
			}()
		} else {
			go func(host string) {
				rsp := DecideRsp{}
				call(host, "Paxos.DecisionHandler", &DecideReq{seq, v, px.me, px.done[px.me]}, &rsp)
				ch <- rsp
			}(p)
		}
	}

	// wait all peers response
	for i := 0; i < len(px.peers); i++ {
		<-ch
	}
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	if seq < px.Min() {
		return
	}

	//goruotine
	go func() {
		for {
			ok, t, nv := px.sendPrepare(seq, v)
			if !ok {
				// if prepare failed retry
				continue
			} else if nv != nil {
				// someone has already have remember v
				v = nv
			}

			ok = px.sendAccept(seq, v, t)

			if !ok {
				// if accept failed retry
				continue
			}

			//done
			px.sendDecision(seq, v)
			break
		}
	}()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	if seq >= px.done[px.me] {
		px.done[px.me] = seq
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	rst := 0
	for i, _ := range px.instance {
		if i > rst {
			rst = i
		}
	}
	return rst
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	rst := 0x3fffffff
	for _, d := range px.done {
		if rst > d {
			rst = d
		}
	}

	//shrink
	for seq, inst := range px.instance {
		if seq < rst && inst.status == Decided {
			delete(px.instance, seq)
		}
	}
	return rst + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	if seq < px.Min() {
		return Forgotten, nil
	}
	px.mu.Lock()
	defer px.mu.Unlock()

	if inst, ok := px.instance[seq]; ok && inst.status == Decided {
		return Decided, inst.value
	}
	return Pending, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.done = make([]int, len(peers))
	px.instance = make(map[int]*PaxosInst)
	for i := 0; i < len(peers); i++ {
		px.done[i] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
