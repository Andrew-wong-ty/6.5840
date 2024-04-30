package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.5840/labrpc"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId  int64
	mutex     sync.Mutex // protects the following
	serialNum uint64     // the sequence number of next command
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
	ck.serialNum = 0
	ck.mutex = sync.Mutex{}
	// Your code here.
	return ck
}

func (ck *Clerk) doGeneralPost(args *PostArgs) {
	ck.mutex.Lock()
	ck.serialNum++
	args.SerialNum = ck.serialNum
	ck.mutex.Unlock()
	args.ClientId = ck.clientId
	args.TransId = nrand()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply PostReply
			ok := srv.Call("ShardCtrler.Post", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.doGeneralPost(&PostArgs{OperationType: JOIN, Servers: servers})
}

func (ck *Clerk) Leave(gids []int) {
	ck.doGeneralPost(&PostArgs{OperationType: LEAVE, GIDs: gids})
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.doGeneralPost(&PostArgs{OperationType: MOVE, Shard: shard, GID: gid})
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	// assign serial number to identify a request
	ck.mutex.Lock()
	ck.serialNum++
	args.SerialNum = ck.serialNum
	ck.mutex.Unlock()
	args.ClientId = ck.clientId
	args.TransId = nrand()
	//log.Printf("trans=%v\n", args.TransId)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			//log.Printf("call ShardCtrler.Query trans=%v\n", args.TransId)
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			//log.Printf("trans=%v, wrongLeader=%v\n", args.TransId, reply.WrongLeader)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
