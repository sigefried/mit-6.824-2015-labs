package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

const CacheExpiredPings = 100

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	kvstore        map[string]string
	operationCache map[string]*CacheData
	isPrimary      bool
	view           viewservice.View
	backup         string
	viewnum        uint
	needSync       bool
	isSynced       bool
	forceSync      bool
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if !pb.isPrimary {
		reply.Err = ErrWrongServer
		return fmt.Errorf(ErrWrongServer)
	}

	//cache, ok := pb.operationCache[args.OpID]
	//if ok {
	//	*reply = cache.Reply.(GetReply)
	//	//return nil
	//}

	// check with backup
	if pb.backup != "" {
		ok := call(pb.backup, "PBServer.BackupGet", args, reply)
		if ok {
			if reply.Err == ErrWrongServer {
				// backup think it is not a backup
				reply.Err = ErrWrongServer
				return nil
			}

		} else {
			// unreliable bakcup
			reply.Err = ErrWrongServer
			return nil
		}
	}

	pb.DoGet(args, reply)
	// record operation
	//newcache := &CacheData{*reply, CacheExpiredPings}
	//pb.operationCache[args.OpID] = newcache
	return nil
}

func (pb *PBServer) BackupGet(args *GetArgs, reply *GetReply) error {

	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.me != pb.view.Backup {
		reply.Err = ErrWrongServer
		return nil
	}

	if !pb.isSynced {
		reply.Err = ErrWrongServer
		return nil
	}

	// detect duplicate
	//cache, ok := pb.operationCache[args.OpID]
	//if ok {
	//	*reply = cache.Reply.(GetReply)
	//	return nil
	//}

	pb.DoGet(args, reply)
	// record operation
	//newcache := &CacheData{*reply, CacheExpiredPings}
	//pb.operationCache[args.OpID] = newcache
	return nil
}

func (pb *PBServer) DoGet(args *GetArgs, reply *GetReply) error {

	value, ok := pb.kvstore[args.Key]
	if ok {
		reply.Err = OK
		reply.Value = value
	} else {
		reply.Err = ErrNoKey
	}

	return nil
}

func (pb *PBServer) ScanCache(args *PutAppendArgs, reply *PutAppendReply) bool {
	// find cache
	cache, ok := pb.operationCache[args.Key]
	if !ok {
		return false
	}

	if args.Method == "Put" {
		method, exist := cache.OpIdToMethod[args.OpID]
		if !exist {
			return false
		} else {
			if method == "Put" {
				*reply = cache.OpIdToReply[args.OpID].(PutAppendReply)
				return true
			} else {
				return false
			}
		}

	} else if args.Method == "Append" {
		method, exist := cache.OpIdToMethod[args.OpID]
		if !exist {
			return false
		} else {
			if method == "Append" {
				*reply = cache.OpIdToReply[args.OpID].(PutAppendReply)
				return true
			} else {
				return false
			}
		}

	}

	return true
}

func (pb *PBServer) UpdateCache(args *PutAppendArgs, reply *PutAppendReply) {
	if args.Method == "Put" {
		cache := new(CacheData)
		cache.OpIdToMethod = make(map[int64]string)
		cache.OpIdToReply = make(map[int64]interface{})
		cache.OpIdToReply[args.OpID] = *reply
		cache.TTL = CacheExpiredPings
		pb.operationCache[args.Key] = cache
	} else if args.Method == "Append" {
		_, ok := pb.operationCache[args.Key]
		if !ok {
			// not exist
			cache := new(CacheData)
			cache.OpIdToMethod = make(map[int64]string)
			cache.OpIdToReply = make(map[int64]interface{})
			cache.OpIdToReply[args.OpID] = *reply
			cache.TTL = CacheExpiredPings
			pb.operationCache[args.Key] = cache
		} else {
			//already exist
			pb.operationCache[args.Key].OpIdToMethod[args.OpID] = "Append"
			pb.operationCache[args.Key].OpIdToReply[args.OpID] = *reply
			pb.operationCache[args.Key].TTL = CacheExpiredPings
		}

	}

}

func (pb *PBServer) DoPutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	if args.Method == "Put" {
		pb.kvstore[args.Key] = args.Value
	} else if args.Method == "Append" {
		pb.kvstore[args.Key] += args.Value
	}
	reply.Err = OK
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if !pb.isPrimary {
		reply.Err = ErrWrongServer
		return fmt.Errorf(ErrWrongServer)
	}

	//cache, ok := pb.operationCache[args.Key]
	//if ok && cache.OpId == args.OpID {
	//	*reply = cache.Reply.(PutAppendReply)
	//	//log.Printf("PutAppend-Cached: {{me: %s method: %s == viewnum: %d, , key: %s value: %s}}\n", pb.me, args.Method, pb.viewnum, args.Key, args.Value)
	//	return nil
	//}
	isHit := pb.ScanCache(args, reply)
	if isHit {
		return nil
	}

	if pb.backup != "" {
		ok := call(pb.backup, "PBServer.BackupPutAppend", args, reply)
		if ok {
			if reply.Err == ErrWrongServer {
				// backup think it is not a backup
				pb.forceSync = true
				reply.Err = ErrWrongServer
				return nil
			}

		} else {
			// unreliable bakcup
			pb.forceSync = true
			reply.Err = ErrWrongServer
			return nil
		}

	}

	pb.DoPutAppend(args, reply)
	//log.Printf("PutAppend: {{me: %s method: %s  == viewnum: %d, key: %s value: %s}}\n", pb.me, args.Method, pb.viewnum, args.Key, args.Value)

	//record operation
	//newcache := &CacheData{*reply, args.OpID, CacheExpiredPings}
	//pb.operationCache[args.Key] = newcache
	pb.UpdateCache(args, reply)

	return nil
}

func (pb *PBServer) BackupPutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.me != pb.view.Backup {
		reply.Err = ErrWrongServer
		return nil
	}

	if !pb.isSynced {
		reply.Err = ErrWrongServer
		return nil
	}

	//cache, ok := pb.operationCache[args.Key]
	//if ok && cache.OpId == args.OpID {
	//	//log.Printf("BackupAppend-cached: {{me: %s == viewnum: %d, key: %s value: %s}}\n", pb.me, pb.viewnum, args.Key, args.Value)
	//	*reply = cache.Reply.(PutAppendReply)
	//	return nil
	//}
	isHit := pb.ScanCache(args, reply)
	if isHit {
		return nil
	}

	//log.Printf("BackupAppend: {{me: %s == viewnum: %d, key: %s value: %s}}\n", pb.me, pb.viewnum, args.Key, args.Value)
	pb.DoPutAppend(args, reply)

	//record operation
	//newcache := &CacheData{*reply, args.OpID, CacheExpiredPings}
	//pb.operationCache[args.Key] = newcache
	pb.UpdateCache(args, reply)
	return nil
}

func (pb *PBServer) SyncHandler(args *SyncReq, reply *SyncRep) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if !pb.isPrimary && pb.isSynced && !args.forced {
		reply.Result = true
		return nil
	}

	if pb.me != pb.view.Backup {
		reply.Result = false
		return nil
	}
	pb.kvstore = args.Data

	reply.Result = true
	pb.isSynced = true
	return nil
}

func (pb *PBServer) SyncData(req *SyncReq, rep *SyncRep) error {
	ok := call(pb.backup, "PBServer.SyncHandler", req, rep)
	if !ok {
		return fmt.Errorf("SyncData Error")
	}

	return nil
}

func (pb *PBServer) CleanCache() {
	for key := range pb.operationCache {
		if pb.operationCache[key].TTL <= 0 {
			delete(pb.operationCache, key)
		} else {
			pb.operationCache[key].TTL--
		}
	}

}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	new_view, e := pb.vs.Ping(pb.viewnum)
	// no error
	if e == nil {
		// copy current view
		pb.viewnum = new_view.Viewnum
		pb.view = new_view
		//log.Printf("tick: {{me: %s == viewnum: %d, primary: %s, backup: %s}}\n", pb.me, pb.view.Viewnum, pb.view.Primary, pb.view.Backup)
		if pb.me == new_view.Primary {
			pb.isPrimary = true
			if new_view.Backup != "" && new_view.Backup != pb.backup {
				pb.backup = new_view.Backup
				pb.needSync = true
				// sync
			}
		} else {
			pb.isPrimary = false
			pb.backup = ""
			pb.needSync = false
		}
	}

	if pb.isPrimary && pb.backup != "" && (pb.needSync || pb.forceSync) {
		if pb.SyncData(&SyncReq{pb.kvstore, pb.forceSync}, &SyncRep{false}) != nil {
			pb.needSync = false
			pb.forceSync = false
		}
	}

	pb.CleanCache()
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.kvstore = make(map[string]string)
	pb.operationCache = make(map[string]*CacheData)
	pb.isPrimary = false
	pb.view = viewservice.View{0, "", ""}
	pb.needSync = true
	pb.isSynced = false
	pb.backup = ""
	pb.viewnum = 0
	pb.forceSync = false

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
