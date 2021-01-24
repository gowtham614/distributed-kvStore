package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Cmd string
	Key string
	Val string
}

type Result struct {
	Err Err
	Cmd string
	Key string
	Val string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	db           map[string]string
	resultCh     map[int]chan Result
	lastAck      map[int64]int64

	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// fmt.Println("Server Get", kv.me)
	var op Op
	op.Key = args.Key
	op.Cmd = "Get"
	var res Result
	kv.sendRaftMsg(op, &res)
	reply.Err = res.Err
	reply.Value = res.Val
}

func (kv *KVServer) sendRaftMsg(op Op, res *Result) {
	idx, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		// fmt.Println("Server Get wrong leader", kv.me)
		res.Err = ErrWrongLeader
		return
	}
	// fmt.Println("Server Get", kv.me, "making channel idx", idx)
	kv.mu.Lock()
	if _, ok := kv.resultCh[idx]; !ok {
		// fmt.Println("Server Get", kv.me, "now making channel idx", idx)
		kv.resultCh[idx] = make(chan Result, 1)
	} else {
		// fmt.Println("Server Get", kv.me, "channel already exist", idx)
	}
	p := kv.resultCh[idx]
	kv.mu.Unlock()

	select {
	case result := <-p:
		if op.Cmd == result.Cmd && op.Key == result.Key { // && op.Val == op.Val {
			*res = result
		}
	case <-time.After(200 * time.Millisecond):
		// fmt.Println("Server Get timeout", kv.me)
		res.Err = ErrNoKey
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// fmt.Println("Server PutAppend", kv.me)
	var op Op
	op.Key = args.Key
	op.Cmd = args.Op
	op.Val = args.Value
	var res Result
	kv.sendRaftMsg(op, &res)
	reply.Err = res.Err
}

func (kv *KVServer) receiveRaftMsg() {
	for !kv.killed() {
		// fmt.Println("Server receiveRaftMsg", kv.me)
		msg := <-kv.applyCh
		kv.mu.Lock()
		if _, ok := kv.resultCh[msg.CommandIndex]; !ok {
			// fmt.Println("Server receiveRaftMsg", kv.me, "no channel found idx", msg.CommandIndex)
			kv.mu.Unlock()
			continue
		}
		op := msg.Command.(Op)
		var res Result
		res.Err = OK
		if op.Cmd == "Put" {
			kv.db[op.Key] = op.Val
		} else if op.Cmd == "Append" {
			kv.db[op.Key] += op.Val
		} else if op.Cmd == "Get" {
			_, ok := kv.db[op.Key]
			if !ok {
				res.Err = ErrNoKey
			}
		}

		res.Cmd = op.Cmd
		res.Key = op.Key
		res.Val = kv.db[op.Key]

		kv.resultCh[msg.CommandIndex] <- res
		// fmt.Println("Server receiveRaftMsg, posting on channel", kv.me)
		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.db = make(map[string]string)
	kv.resultCh = make(map[int]chan Result)

	// You may need initialization code here.
	go kv.receiveRaftMsg()

	return kv
}
