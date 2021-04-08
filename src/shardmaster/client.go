package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	mu       sync.Mutex
	leaderId int
	me       int64
	opID     int64
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
	ck.leaderId = 0
	ck.opID = 1
	ck.me = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	ck.mu.Lock()
	args.OpID = ck.opID
	ck.opID++
	ck.mu.Unlock()
	args.ClientID = ck.me
	for {
		// try each known server.
		for range ck.servers {
			var reply QueryReply
			ok := ck.servers[ck.leaderId].Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	ck.mu.Lock()
	args.OpID = ck.opID
	ck.opID++
	ck.mu.Unlock()
	args.ClientID = ck.me

	for {
		// try each known server.
		for range ck.servers {
			var reply JoinReply
			ok := ck.servers[ck.leaderId].Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	ck.mu.Lock()
	args.OpID = ck.opID
	ck.opID++
	ck.mu.Unlock()
	args.ClientID = ck.me

	for {
		// try each known server.
		for range ck.servers {
			var reply LeaveReply
			ok := ck.servers[ck.leaderId].Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	ck.mu.Lock()
	args.OpID = ck.opID
	ck.opID++
	ck.mu.Unlock()
	args.ClientID = ck.me

	for {
		// try each known server.
		for range ck.servers {
			var reply MoveReply
			ok := ck.servers[ck.leaderId].Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
