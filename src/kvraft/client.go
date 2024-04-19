package kvraft

import (
	"6.5840/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId  int64
	serialNum int // the sequence number of next command
	leaderId  int
	mutex     sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.serialNum = 1
	ck.leaderId = 0
	ck.mutex = sync.Mutex{}
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	return ck.doCommand(CommandRequest{
		Key:       key,
		Value:     "",
		OpType:    GET,
		SerialNum: ck.serialNum,
		ClientId:  ck.clientId,
	})
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.doCommand(CommandRequest{
		Key:       key,
		Value:     value,
		OpType:    op,
		SerialNum: ck.serialNum,
		ClientId:  ck.clientId,
	})
}

func (ck *Clerk) doCommand(req CommandRequest) string {
	ck.mutex.Lock()
	leader := ck.leaderId
	ck.mutex.Unlock()
	for {
		rsp := CommandResponse{}
		req.TransId = nrand()
		ok := ck.servers[leader].Call("KVServer.CommandHandler", &req, &rsp)
		if ok {
			if rsp.Err == OK || rsp.Err == ErrNoKey {
				ck.mutex.Lock()
				ck.leaderId = leader
				ck.serialNum++
				ck.mutex.Unlock()
				return rsp.Value
			}
		}
		leader = (leader + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
