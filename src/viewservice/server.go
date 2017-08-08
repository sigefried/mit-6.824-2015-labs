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
	curView      View
	isPrimaryAck bool
	nextView     View
	serverBeat   map[string]time.Time
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// server beat update
	vs.serverBeat[args.Me] = time.Now()

	if args.Me == vs.curView.Primary {
		// deal with primary fails come back immediately
		if args.Viewnum == 0 {
			if vs.curView.Backup == "" {
				vs.nextView.Primary = ""
				vs.nextView.Backup = ""
			} else {
				vs.nextView.Primary = vs.curView.Backup
				vs.nextView.Backup = args.Me
			}
			vs.nextView.Viewnum = vs.curView.Viewnum + 1
		}

		// process primary ack
		if args.Viewnum == vs.curView.Viewnum {
			vs.isPrimaryAck = true
		}
	} else if args.Me == vs.curView.Backup {
		// do nothing

	} else {
		// new server

		// init state
		if vs.curView.Primary == "" && vs.curView.Viewnum == 0 {
			vs.curView.Primary = args.Me
			vs.curView.Viewnum++
			vs.nextView = vs.curView
		} else if vs.curView.Backup == "" {
			// no backup
			vs.nextView.Primary = vs.curView.Primary
			vs.nextView.Backup = args.Me
			vs.nextView.Viewnum = vs.curView.Viewnum + 1
		}

	}

	if vs.isPrimaryAck && vs.curView.Viewnum < vs.nextView.Viewnum {
		vs.curView = vs.nextView
		vs.isPrimaryAck = false
		vs.nextView = View{0, "", ""}
	}
	reply.View = vs.curView

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	reply.View = vs.curView

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
	idleServer := make([]string, 0)
	cur := time.Now().UnixNano()
	for name, t := range vs.serverBeat {
		if (cur-t.UnixNano())/int64(PingInterval) >= DeadPings {
			if name == vs.curView.Primary && vs.isPrimaryAck {
				vs.nextView.Primary, vs.nextView.Backup = vs.curView.Backup, ""
				vs.nextView.Viewnum = vs.curView.Viewnum + 1
			} else if name == vs.curView.Backup && vs.isPrimaryAck {
				vs.nextView.Primary, vs.nextView.Backup = vs.curView.Primary, ""
				vs.nextView.Viewnum = vs.curView.Viewnum + 1
			}
		} else if (cur-t.UnixNano())/int64(PingInterval) < DeadPings {
			idleServer = append(idleServer, name)
		}
	}

	if vs.nextView.Backup == "" {
		for _, name := range idleServer {
			if name != vs.nextView.Primary {
				vs.nextView.Backup = name
				break
			}
		}
	}
	if vs.isPrimaryAck && vs.curView.Viewnum < vs.nextView.Viewnum {
		vs.curView = vs.nextView
		vs.isPrimaryAck = false
		vs.nextView = View{0, "", ""}
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
	vs.curView = View{0, "", ""}
	vs.nextView = View{0, "", ""}
	vs.isPrimaryAck = true
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
