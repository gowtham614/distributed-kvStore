package kvraft

import (
	"bytes"
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
	Cmd      string
	Key      string
	Val      string
	ClientID int64
	OpID     int64
}

type Result struct {
	Err      Err
	Cmd      string
	Key      string
	Val      string
	ClientID int64
	OpID     int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate         int // snapshot if log grows this big
	db                   map[string]string
	resultCh             map[int]chan Result
	lastAck              map[int64]int64
	lastRaftCommandIndex int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	var op Op
	op.Key = args.Key
	op.Cmd = "Get"
	op.ClientID = args.ClientID
	op.OpID = args.OpID
	var res Result
	kv.sendRaftMsg(op, &res)
	reply.Err = res.Err
	reply.Value = res.Val
}

func (kv *KVServer) sendRaftMsg(op Op, res *Result) {
	idx, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if _, ok := kv.resultCh[idx]; !ok {
		kv.resultCh[idx] = make(chan Result, 1)
	}
	p := kv.resultCh[idx]
	kv.mu.Unlock()

	select {
	case result := <-p:
		if op.Cmd != result.Cmd || op.Key != result.Key || op.ClientID != result.ClientID || op.OpID != result.OpID {
			res.Err = ErrWrongLeader
			return
		}

		res.Cmd = result.Cmd
		res.Key = result.Key
		res.Val = result.Val
		res.Err = result.Err

	case <-time.After(2000 * time.Millisecond):
		res.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	var op Op
	op.Key = args.Key
	op.Cmd = args.Op
	op.Val = args.Value
	op.ClientID = args.ClientID
	op.OpID = args.OpID

	kv.mu.Lock()
	id := kv.lastAck[op.ClientID]
	kv.mu.Unlock()
	if id >= op.OpID {
		reply.Err = OK
		return
	}
	var res Result
	kv.sendRaftMsg(op, &res)
	reply.Err = res.Err
}

// thread for receiving all raft msg
func (kv *KVServer) receiveRaftMsg() {
	for !kv.killed() {
		msg := <-kv.applyCh
		kv.mu.Lock()

		if !msg.CommandValid {
			// recover from snapshot
			r := bytes.NewBuffer(msg.Command.([]byte))
			d := labgob.NewDecoder(r)
			d.Decode(&kv.db)
			d.Decode(&kv.lastAck)
			kv.lastRaftCommandIndex = msg.CommandIndex
			kv.mu.Unlock()
			continue
		}
		op := msg.Command.(Op)
		var res Result
		res.Err = OK
		res.ClientID = op.ClientID
		res.OpID = op.OpID

		// updating internal key value store database
		if op.Cmd == "Put" {
			if kv.lastAck[op.ClientID] < op.OpID {
				kv.db[op.Key] = op.Val
			}
		} else if op.Cmd == "Append" {
			if kv.lastAck[op.ClientID] < op.OpID {
				kv.db[op.Key] += op.Val
			}
		} else if op.Cmd == "Get" {
			_, ok := kv.db[op.Key]
			if !ok {
				res.Err = ErrNoKey
			}
		}

		res.Cmd = op.Cmd
		res.Key = op.Key
		res.Val = kv.db[op.Key]
		kv.lastRaftCommandIndex = msg.CommandIndex
		kv.lastAck[op.ClientID] = op.OpID
		if _, ok := kv.resultCh[msg.CommandIndex]; ok {
			kv.resultCh[msg.CommandIndex] <- res
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) takeSnapshot() {
	currentCommandIndex := 0
	for !kv.killed() {
		kv.mu.Lock()
		if kv.maxraftstate > 0 && kv.rf.GetRaftStateSize() > kv.maxraftstate && kv.lastRaftCommandIndex > currentCommandIndex { // threshold??
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.db)
			e.Encode(kv.lastAck)
			currentCommandIndex = kv.lastRaftCommandIndex
			if !kv.rf.StoreSnapshot(w.Bytes(), kv.lastRaftCommandIndex) { // we need to store the command Index
			}
		}
		time.Sleep(time.Millisecond)
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

	kv.applyCh = make(chan raft.ApplyMsg, 10)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.db = make(map[string]string)
	kv.resultCh = make(map[int]chan Result)
	kv.lastAck = make(map[int64]int64)

	// You may need initialization code here.
	go kv.receiveRaftMsg()
	go kv.takeSnapshot()

	return kv
}
