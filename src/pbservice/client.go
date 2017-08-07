package pbservice

import "viewservice"
import "net/rpc"
import "fmt"

import "time"

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	view *viewservice.View
}

// this may come in handy.

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	ck.view = nil

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

func (ck *Clerk) GetView(rpccall bool) *viewservice.View {
	if ck.view == nil || rpccall {
		v, ok := ck.vs.Get()
		if ok {
			ck.view = &v
		} else {
			ck.view = nil
		}
	}
	return ck.view
}

func (ck *Clerk) GetPrimary(rpccall bool) string {
	v := ck.GetView(rpccall)
	for v == nil {
		v = ck.GetView(rpccall)
		time.Sleep(viewservice.PingInterval)
		rpccall = true
	}
	return v.Primary
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

	// Your code here.
	getArgs := &GetArgs{key, nrand()}
	var reply GetReply

	rpccall := false
	for {
		primary := ck.GetPrimary(rpccall)

		call(primary, "PBServer.Get", getArgs, &reply)

		if reply.Err == OK || reply.Err == ErrNoKey {
			break
		}

		rpccall = true
		time.Sleep(viewservice.PingInterval)
	}

	return reply.Value
}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	// Your code here.
	putArgs := &PutAppendArgs{key, value, op, nrand()}
	var reply PutAppendReply

	rpccall := false
	for {
		primary := ck.GetPrimary(rpccall)

		call(primary, "PBServer.PutAppend", putArgs, &reply)
		//DPrintf("(viewnum: %d, primary: %s, backup: %s)\n", ck.view.Viewnum, ck.view.Primary, ck.view.Backup)

		if reply.Err == OK {
			break
		}
		rpccall = true
		time.Sleep(viewservice.PingInterval)
	}
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
