package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	primary  string
	backup   string
	viewnum  uint
	lastview uint

	serverBeat map[string]time.Time
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.primary == "" {
		vs.viewnum++
		vs.primary = args.Me
	} else if vs.backup == "" && vs.primary != args.Me {
		vs.viewnum++
		vs.backup = args.Me
	} else if vs.primary == args.Me && args.Viewnum == 0 {
		vs.primary, vs.backup = vs.backup, vs.primary
		vs.viewnum++
	}

	if args.Me == vs.primary {
		vs.lastview = args.Viewnum
	}
	vs.serverBeat[args.Me] = time.Now()
	reply.View.Backup = vs.backup
	reply.View.Primary = vs.primary
	reply.View.Viewnum = vs.viewnum

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	reply.View.Primary = vs.primary
	reply.View.Backup = vs.backup
	reply.View.Viewnum = vs.viewnum

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	// Your code here.

	idle := make([]string, 0)
	cur := time.Now().UnixNano()
	for n, d := range vs.serverBeat {
		if (cur-d.UnixNano())/int64(PingInterval) >= DeadPings &&
			vs.lastview == vs.viewnum {
			if n == vs.primary {
				vs.primary, vs.backup = vs.backup, ""
				vs.viewnum++
			} else if n == vs.backup {
				vs.backup = ""
				vs.viewnum++
			}
		} else if (cur-d.UnixNano())/int64(PingInterval) < DeadPings {
			idle = append(idle, n)
		}
	}

	if vs.backup == "" {
		for _, d := range idle {
			if d != vs.primary {
				//DPrintf("----backup name: %s\n", d)
				vs.backup = d
				break
			}
		}
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.viewnum = 0
	vs.lastview = 0
	vs.serverBeat = make(map[string]time.Time)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
