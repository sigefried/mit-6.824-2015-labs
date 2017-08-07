package pbservice

import "crypto/rand"
import "math/big"
import "fmt"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
	ErrNotSync     = "ErrNotSync"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Method string
	OpID   int64

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type CacheData struct {
	Reply interface{}
	TTL   int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	OpID int64
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.

type SyncReq struct {
	Data   map[string]string
	forced bool
}

type SyncRep struct {
	Result bool
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
