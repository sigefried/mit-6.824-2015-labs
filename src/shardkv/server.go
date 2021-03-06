package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	Get    = "Get"
	Put    = "Put"
	Append = "Append"
	Reconf = "Reconf"
)

type Op struct {
	// Your definitions here.
	CID   string //client ID
	Seq   int    //client seq
	Op    string
	Key   string
	Value string
	Extra interface{}
}

func (op *Op) IsSame(other *Op) bool {
	if op.Op == other.Op {
		if op.Op == Reconf {
			// Seq refers to config_num in 'Reconf' cases
			return op.Seq == other.Seq
		}
		return op.CID == other.CID && op.Seq == other.Seq
	}
	return false
}

type Rep struct {
	Err   Err
	Value string
}

type XState struct {
	// key-value store
	KVStore map[string]string

	// client states for filtering duplicate ops
	// map client -> the Most Recent Request Seq of the client
	MRRSMap map[string]int
	// map client -> the most recent reply to the client
	Replies map[string]Rep
}

func (xs *XState) Init() {
	xs.KVStore = map[string]string{}
	xs.MRRSMap = map[string]int{}
	xs.Replies = map[string]Rep{}
}

func MakeXState() *XState {
	var xstate XState
	xstate.Init()
	return &xstate
}

func (xs *XState) Update(other *XState) {
	for key, value := range other.KVStore {
		xs.KVStore[key] = value
	}

	for cli, seq := range other.MRRSMap {
		xseq := xs.MRRSMap[cli]
		if xseq < seq {
			xs.MRRSMap[cli] = seq
			xs.Replies[cli] = other.Replies[cli]
		}
	}
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.

	config shardmaster.Config

	lastSeq int
	seq     int
	xstate  XState
}

func (kv *ShardKV) wait(seq int) Op {
	to := 10 * time.Millisecond
	for {
		fate, v := kv.px.Status(seq)
		if fate == paxos.Decided {
			return v.(Op)
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}

}

func (kv *ShardKV) applyOp(op *Op) {
	var rep *Rep
	switch op.Op {
	case Reconf:
		kv.config = kv.sm.Query(op.Seq)
		extra := op.Extra.(XState)
		kv.xstate.Update(&extra)
	case Put, Append:
		rep = kv.doPutAppend(op.Op, op.Key, op.Value)
		kv.recordOperation(op.CID, op.Seq, rep)
	case Get:
		rep = kv.doGet(op.Key)
		kv.recordOperation(op.CID, op.Seq, rep)
	}
}

func (kv *ShardKV) sync(o *Op) {
	seq := kv.seq
	var ro Op
	for {
		fate, v := kv.px.Status(seq)
		if fate == paxos.Decided {
			ro = v.(Op)
		} else {
			kv.px.Start(seq, *o)
			ro = kv.wait(seq)
		}
		kv.applyOp(&ro)
		kv.px.Done(seq)
		kv.lastSeq = seq + 1
		seq++
		kv.seq++
		if o.IsSame(&ro) {
			break
		}
	}
	kv.seq = seq
}

func (kv *ShardKV) fetchCachedResult(cid string, seq int) (*Rep, bool) {
	lastSeq, ok := kv.xstate.MRRSMap[cid]
	if !ok {
		return nil, false
	}
	if seq < lastSeq {
		return nil, true
	} else if seq == lastSeq {
		rep, _ := kv.xstate.Replies[cid]
		return &rep, true
	}
	return nil, false
}

func (kv *ShardKV) recordOperation(cid string, seq int, reply *Rep) {
	if reply.Err != ErrWrongGroup {
		kv.xstate.MRRSMap[cid] = seq
		kv.xstate.Replies[cid] = *reply
	}
}

func (kv *ShardKV) doGet(key string) *Rep {
	var rep Rep
	if kv.gid != kv.config.Shards[key2shard(key)] {
		rep.Err = ErrWrongGroup
	} else {
		value, ok := kv.xstate.KVStore[key]
		if ok {
			rep.Err, rep.Value = OK, value
		} else {
			rep.Err = ErrNoKey
		}
	}
	return &rep
}

func (kv *ShardKV) doPutAppend(op string, key string, value string) *Rep {
	var rep Rep
	if kv.gid != kv.config.Shards[key2shard(key)] {
		rep.Err = ErrWrongGroup
	} else {
		if op == Put {
			kv.xstate.KVStore[key] = value
		} else if op == Append {
			kv.xstate.KVStore[key] += value
		}
		rep.Err = OK
	}
	return &rep
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	rep, ok := kv.fetchCachedResult(args.CID, args.Seq)

	if ok {
		if rep != nil {
			reply.Err, reply.Value = rep.Err, rep.Value
		}
		return nil
	}
	xop := &Op{CID: args.CID, Seq: args.Seq, Op: Get, Key: args.Key}
	kv.sync(xop)

	//get cached result
	rep, ok = kv.fetchCachedResult(args.CID, args.Seq)
	if !ok {
		reply.Err = ErrWrongGroup
	} else {
		reply.Err, reply.Value = rep.Err, rep.Value
	}

	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	rep, ok := kv.fetchCachedResult(args.CID, args.Seq)

	if ok {
		if rep != nil {
			reply.Err = rep.Err
		}
		return nil
	}

	xop := &Op{CID: args.CID, Seq: args.Seq, Op: args.Op, Key: args.Key, Value: args.Value}
	kv.sync(xop)

	//get cached result
	rep, ok = kv.fetchCachedResult(args.CID, args.Seq)
	if !ok {
		reply.Err = ErrWrongGroup
	} else {
		reply.Err = rep.Err
	}
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	latest_config := kv.sm.Query(-1)

	//apply the configuration one by one
	for n := kv.config.Num + 1; n <= latest_config.Num; n++ {
		config := kv.sm.Query(n)
		if !kv.reconfigure(&config) {
			break
		}
	}
}

func (kv *ShardKV) reconfigure(config *shardmaster.Config) bool {
	xstate := MakeXState()

	for shard := 0; shard < shardmaster.NShards; shard++ {

		// current state gid
		gid := kv.config.Shards[shard]

		if config.Shards[shard] == kv.gid && gid != 0 && gid != kv.gid {
			ret := kv.requestShard(gid, shard)
			if ret == nil {
				return false
			}
			xstate.Update(ret)
		}
	}

	xop := &Op{Seq: config.Num, Op: Reconf, Extra: *xstate}
	kv.sync(xop)

	return true
}

func (kv *ShardKV) requestShard(gid int64, shard int) *XState {
	for _, server := range kv.config.Groups[gid] {
		args := &TransferStateArgs{}
		args.ConfigNum, args.Shard = kv.config.Num, shard
		var reply TransferStateReply
		ok := call(server, "ShardKV.TransferState", args, &reply)
		if ok && reply.Err == OK {
			return &reply.XState
		}
	}

	return nil
}

func (kv *ShardKV) TransferState(args *TransferStateArgs, reply *TransferStateReply) error {
	if kv.config.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return nil
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.XState.Init()

	for key := range kv.xstate.KVStore {
		if key2shard(key) == args.Shard {
			value := kv.xstate.KVStore[key]
			reply.XState.KVStore[key] = value
		}
	}
	for client := range kv.xstate.MRRSMap {
		reply.XState.MRRSMap[client] = kv.xstate.MRRSMap[client]
		reply.XState.Replies[client] = kv.xstate.Replies[client]
	}

	reply.Err = OK
	return nil

}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})
	gob.Register(XState{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)
	kv.seq = 0
	kv.lastSeq = 0

	kv.xstate.Init()

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
