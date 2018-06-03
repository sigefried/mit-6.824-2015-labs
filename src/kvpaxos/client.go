package kvpaxos

import "net/rpc"
import "crypto/rand"
import "math/big"

import "fmt"
import "time"

type Clerk struct {
	servers []string
	// You will have to modify this struct.
	me string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	time.Sleep(10 * time.Millisecond) // make different NanoSeconds
	ck.me = fmt.Sprintf("%d_%d", time.Now().UnixNano(), nrand())

	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	i := 0
	req := GetArgs{key, nrand()}
	rsp := GetReply{}
	for {
		DPrintf("[Client]: client: %s Get %d\n", ck.me, req.OpID)
		addr := ck.servers[i]

		ok := call(addr, "KVPaxos.Get", &req, &rsp)
		if ok {
			if rsp.Err == OK {
				return rsp.Value
			} else if rsp.Err == ErrNoKey {
				return ""
			}
		}

		i = (i + 1) % len(ck.servers)
	}
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	i := 0
	req := PutAppendArgs{key, value, op, nrand()}
	rsp := PutAppendReply{}
	for {
		DPrintf("[Client]: client: %s PutAppend %d\n", ck.me, req.OpID)
		addr := ck.servers[i]

		ok := call(addr, "KVPaxos.PutAppend", &req, &rsp)
		if ok && rsp.Err == OK {
			return
		}

		i = (i + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
