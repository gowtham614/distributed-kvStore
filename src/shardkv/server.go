package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
)

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

type ShardKV struct {
	mu                   sync.Mutex
	me                   int
	rf                   *raft.Raft
	applyCh              chan raft.ApplyMsg
	dead                 int32
	make_end             func(string) *labrpc.ClientEnd
	gid                  int
	masters              []*labrpc.ClientEnd
	maxraftstate         int // snapshot if log grows this big
	db                   [shardmaster.NShards]map[string]string
	resultCh             map[int]chan Result
	lastAck              map[int64]int64
	lastRaftCommandIndex int
	config               shardmaster.Config
	sm                   *shardmaster.Clerk
	// Your definitions here.
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// fmt.Println("kv ", kv.me, "Get ", args.ClientID, args.OpID, "key =", args.Key)
	kv.mu.Lock()
	validKey := kv.validKey(args.Key)
	kv.mu.Unlock()
	if !validKey {
		reply.Err = ErrWrongGroup
		return
	}
	var op Op
	op.Key = args.Key
	op.Cmd = "Get"
	op.ClientID = args.ClientID
	op.OpID = args.OpID
	var res Result
	kv.sendRaftMsg(op, &res)
	reply.Err = res.Err
	reply.Value = res.Val
	if len(reply.Value) > 10 {
		// fmt.Println("kv", kv.me, "reply get, ", args.ClientID, args.OpID, "key =", args.Key, ",", reply.Value[len(reply.Value)-10:])
	} else {
		// fmt.Println("kv", kv.me, "reply get, ", args.ClientID, args.OpID, "key =", args.Key, ",", reply.Value)
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// fmt.Println("kv", kv.me, " put append, ", args.ClientID, args.OpID, "key =", args.Key, args.Value)
	var op Op
	op.Key = args.Key
	op.Cmd = args.Op
	op.Val = args.Value
	op.ClientID = args.ClientID
	op.OpID = args.OpID

	kv.mu.Lock()
	id := kv.lastAck[op.ClientID]
	validKey := kv.validKey(args.Key)
	kv.mu.Unlock()
	if id >= op.OpID {
		// fmt.Println("duplicate put append, ", kv.me, args.ClientID, args.OpID, "key =", args.Key, args.Value, kv.lastAck)
		reply.Err = OK
		return
	}
	if !validKey {
		reply.Err = ErrWrongGroup
		return
	}
	var res Result
	kv.sendRaftMsg(op, &res)
	reply.Err = res.Err
}

// look should be held before calling
func (kv *ShardKV) validKey(key string) bool {
	return kv.config.Shards[key2shard(key)] == kv.gid
}

func (kv *ShardKV) sendRaftMsg(op Op, res *Result) {
	// fmt.Println("kv", kv.me, "sendRaftMsg sending start() ", op)
	idx, _, isLeader := kv.rf.Start(op)
	// fmt.Println("kv", kv.me, " sendRaftMsg start() finished", op)
	if !isLeader {
		// fmt.Println("kv", kv.me, " sendRaftMsg Get wrong leader")
		res.Err = ErrWrongLeader
		return
	}
	// fmt.Println("Server Get", kv.me, "making channel idx", idx)
	kv.mu.Lock()
	if _, ok := kv.resultCh[idx]; !ok {
		// fmt.Println("Server Get", kv.me, "now making channel idx", idx)
		kv.resultCh[idx] = make(chan Result, 1)
	} else {
		// fmt.Println("kv", kv.me, "sendRaftMsg channel already exist", idx)
	}
	p := kv.resultCh[idx]
	kv.mu.Unlock()

	select {
	case result := <-p:
		if op.Cmd != result.Cmd || op.Key != result.Key || op.ClientID != result.ClientID || op.OpID != result.OpID {
			// fmt.Println("kv", kv.me, "sendRaftMsg no key", op.ClientID, op.OpID, op.Cmd, "key =", op.Key, "res", result.Cmd, result.Key)
			res.Err = ErrWrongLeader
			return
		}

		res.Cmd = result.Cmd
		res.Key = result.Key
		res.Val = result.Val
		res.Err = result.Err

		// fmt.Println("kv", kv.me, "sendRaftMsg sucess", op.ClientID, op.OpID, op.Cmd, "key =", op.Key, res.Val)
	case <-time.After(2000 * time.Millisecond):
		// fmt.Println("kv", kv.me, "sendRaftMsg timeout wrongleader", op.ClientID, op.OpID, op.Cmd, "key =", op.Key)
		res.Err = ErrWrongLeader
	}
}

// thread for receiving all raft msg
func (kv *ShardKV) receiveRaftMsg() {
	for !kv.killed() {
		// fmt.Println("kv", kv.me, "Server receiveRaftMsg waiting")
		msg := <-kv.applyCh
		// fmt.Println("kv", kv.me, "Server receiveRaftMsg received")
		kv.mu.Lock()

		if !msg.CommandValid {
			// recover from snapshot
			// fmt.Println("kv", kv.me, " recover from snapshot")
			r := bytes.NewBuffer(msg.Command.([]byte))
			d := labgob.NewDecoder(r)
			d.Decode(&kv.db)
			d.Decode(&kv.lastAck)
			// fmt.Println("kv", kv.me, " after recover", "db =", kv.db)
			kv.lastRaftCommandIndex = msg.CommandIndex
			kv.mu.Unlock()
			continue
		}
		op := msg.Command.(Op)
		// fmt.Println("kv", kv.me, " Server receiveraft Msg", "ClientID", op.ClientID,
		// 	"cmd", op.Cmd, "key", op.Key, "opid", op.OpID, "val", op.Val)
		var res Result
		res.Err = OK
		res.ClientID = op.ClientID
		res.OpID = op.OpID

		// updating internal key value store database
		if op.Cmd == "Put" {
			if kv.lastAck[op.ClientID] < op.OpID {
				kv.db[key2shard(op.Key)][op.Key] = op.Val
			} else {
				// fmt.Println("kv ", kv.me, "duplicate receiveRaftMsg put, ", op.ClientID, op.OpID, "key =", op.Key, ",", op.Val, kv.lastAck)
			}
		} else if op.Cmd == "Append" {
			if kv.lastAck[op.ClientID] < op.OpID {
				kv.db[key2shard(op.Key)][op.Key] += op.Val
			} else {
				// fmt.Println("kv ", kv.me, "duplicate receiveRaftMsg append, ", op.ClientID, op.OpID, "key =", op.Key, ",", op.Val, kv.lastAck)
			}
		} else if op.Cmd == "Get" {
			_, ok := kv.db[key2shard(op.Key)][op.Key]
			if !ok {
				// fmt.Println("kv ", kv.me, "receiveRaftMsg no key", op.ClientID, op.OpID, op.Cmd, "key =", op.Key, kv.lastAck)
				res.Err = ErrNoKey
			}
		}

		res.Cmd = op.Cmd
		res.Key = op.Key
		res.Val = kv.db[key2shard(op.Key)][op.Key]
		kv.lastRaftCommandIndex = msg.CommandIndex
		kv.lastAck[op.ClientID] = op.OpID
		if _, ok := kv.resultCh[msg.CommandIndex]; ok {
			// fmt.Println("kv", kv.me, " Server receiveRaftMsg, posting on channel waiting")
			kv.resultCh[msg.CommandIndex] <- res
			// fmt.Println("kv", kv.me, " Server receiveRaftMsg, posting on channel finished")
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) takeSnapshot() {
	currentCommandIndex := 0
	for !kv.killed() {
		kv.mu.Lock()
		if kv.maxraftstate > 0 && kv.rf.GetRaftStateSize() > kv.maxraftstate && kv.lastRaftCommandIndex > currentCommandIndex { // threshold??
			// fmt.Println("kv", kv.me, "snap shot")
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.db)
			e.Encode(kv.lastAck)
			currentCommandIndex = kv.lastRaftCommandIndex
			if !kv.rf.StoreSnapshot(w.Bytes(), kv.lastRaftCommandIndex) { // we need to store the command Index
				// fmt.Println("kv", kv.me, "snap shot failed")
			}
		}
		time.Sleep(time.Millisecond)
		kv.mu.Unlock()
		// todo fix this
		// time.Sleep(time.Millisecond)
	}
}

// thread for getConfig
func (kv *ShardKV) getConfig() {
	for !kv.killed() {
		kv.mu.Lock()
		kv.config = kv.sm.Query(-1)
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	kv.sm = shardmaster.MakeClerk(kv.masters)
	kv.config = kv.sm.Query(-1)

	kv.applyCh = make(chan raft.ApplyMsg, 10)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.db[i] = make(map[string]string)
	}
	kv.resultCh = make(map[int]chan Result)
	kv.lastAck = make(map[int64]int64)

	go kv.receiveRaftMsg()
	go kv.takeSnapshot()
	go kv.getConfig()
	return kv
}
