package kvraft

import (
	"crypto/rand"
	"math/big"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	ck.opID = 0
	ck.me = nrand()
	return ck
}

//
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
//
func (ck *Clerk) Get(key string) string {
	// fmt.Println("Client Get", key)
	var args GetArgs
	args.Key = key
	var reply GetReply
	reply.Err = ErrWrongLeader
	// we should randomly call a server, check for leader
	for reply.Err != OK {

		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		// fmt.Println("Client Get leader id", ck.leaderId, reply.Err, ok)
		if reply.Err == OK {
			return reply.Value
		}
		if reply.Err == ErrNoKey {
			return ""
		}
		if reply.Err == ErrWrongLeader || !ok {
			ck.leaderId = int(nrand()) % len(ck.servers)
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// fmt.Println("Client PutAppend", key, value)
	var args PutAppendArgs
	args.Op = op
	args.Value = value
	args.Key = key
	var reply PutAppendReply
	// we should randomly call a server, check for leader
	for reply.Err != OK {
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if reply.Err == OK {
			return
		}
		if reply.Err == ErrWrongLeader || !ok {
			ck.leaderId = int(nrand()) % len(ck.servers)
		}
	}
	// fmt.Println("Client PutAppend return", key, value)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
