package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"

import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num

	seq int
}

const (
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
	Query = "Query"
)

type Op struct {
	Op      string
	GID     int64
	Servers []string
	Shard   int
	OpID    int64
}

func (sm *ShardMaster) newconfig() *Config {
	config := Config{Num: len(sm.configs), Groups: make(map[int64][]string)}
	if len(sm.configs) > 0 {
		for i, j := range sm.configs[len(sm.configs)-1].Shards {
			config.Shards[i] = j
		}

		for gid, server := range sm.configs[len(sm.configs)-1].Groups {
			config.Groups[gid] = server
		}
	}
	return &config
}

func (sm *ShardMaster) doMove(shard int, gid int64) {
	config := sm.newconfig()
	config.Shards[shard] = gid
	sm.configs = append(sm.configs, *config)
}

func (sm *ShardMaster) doJoin(gid int64, servers []string) {
	config := sm.newconfig()
	_, exists := config.Groups[gid]
	if !exists {
		config.Groups[gid] = servers
		sm.rebalance(config, Join, gid)
	}
	sm.configs = append(sm.configs, *config)
}

func (sm *ShardMaster) doLeave(gid int64) {
	config := sm.newconfig()
	_, exists := config.Groups[gid]
	if exists {
		delete(config.Groups, gid)
		sm.rebalance(config, Leave, gid)
	}
	sm.configs = append(sm.configs, *config)
}

func (sm *ShardMaster) applyOp(o Op) {
	switch o.Op {
	case Join:
		sm.doJoin(o.GID, o.Servers)
	case Leave:
		sm.doLeave(o.GID)
	case Move:
		sm.doMove(o.Shard, o.GID)
	default:
	}

}

func (sm *ShardMaster) rebalance(config *Config, op string, gid int64) {
	count_map := make(map[int64]int)
	shard_map := make(map[int64][]int)
	for shard, xgid := range config.Shards {
		count_map[xgid] += 1
		shard_map[xgid] = append(shard_map[xgid], shard)
	}
	max_nshards, max_gid := 0, int64(0)
	min_nshards, min_gid := NShards+1, int64(0)
	for xgid := range config.Groups {
		nshards := count_map[xgid]

		if max_nshards < nshards {
			max_nshards, max_gid = nshards, xgid
		}
		if min_nshards > nshards {
			min_nshards, min_gid = nshards, xgid
		}
	}

	if op == Join {
		spg := NShards / len(config.Groups)
		for i := 0; i < spg; i++ {
			shard := shard_map[max_gid][i]
			config.Shards[shard] = gid
		}
	} else if op == Leave {
		for _, shard := range shard_map[gid] {
			config.Shards[shard] = min_gid
		}
	}
}

func (sm *ShardMaster) wait(seq int) Op {
	to := 10 * time.Millisecond
	for {
		fate, v := sm.px.Status(seq)
		if fate == paxos.Decided {
			return v.(Op)
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}

}

func (sm *ShardMaster) sync(o Op) {
	var ro Op
	for {
		fate, v := sm.px.Status(sm.seq)
		if fate == paxos.Decided {
			ro = v.(Op)
		} else {
			sm.px.Start(sm.seq, o)
			ro = sm.wait(sm.seq)
		}

		sm.applyOp(ro)
		sm.px.Done(sm.seq)
		sm.seq++
		if ro.OpID == o.OpID {
			break
		}
	}

}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.sync(Op{Join, args.GID, args.Servers, 0, nrand()})

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.sync(Op{Leave, args.GID, nil, 0, nrand()})

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.sync(Op{Move, args.GID, nil, args.Shard, nrand()})

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.sync(Op{Query, 0, nil, 0, nrand()})
	if args.Num == -1 || args.Num >= len(sm.configs) {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[args.Num]

	}

	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.
	sm.seq = 0

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
