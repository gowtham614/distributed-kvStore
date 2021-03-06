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

	DB      [shardmaster.NShards]map[string]string
	LastAck [shardmaster.NShards]map[int64]int64
	Config  shardmaster.Config
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
	lastAck              [shardmaster.NShards]map[int64]int64
	lastRaftCommandIndex int
	config               shardmaster.Config
	sm                   *shardmaster.Clerk
	// Your definitions here.
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
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
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	var op Op
	op.Key = args.Key
	op.Cmd = args.Op
	op.Val = args.Value
	op.ClientID = args.ClientID
	op.OpID = args.OpID

	kv.mu.Lock()
	id := kv.lastAck[key2shard(args.Key)][op.ClientID]
	validKey := kv.validKey(args.Key)
	kv.mu.Unlock()
	if !validKey {
		reply.Err = ErrWrongGroup
		return
	}
	if id >= op.OpID {
		reply.Err = OK
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

// thread for receiving all raft msg
func (kv *ShardKV) receiveRaftMsg() {
	for !kv.killed() {
		msg := <-kv.applyCh
		kv.mu.Lock()

		if !msg.CommandValid {
			// recover from snapshot
			r := bytes.NewBuffer(msg.Command.([]byte))
			d := labgob.NewDecoder(r)
			d.Decode(&kv.db)
			d.Decode(&kv.lastAck)
			d.Decode(&kv.config)
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
			if !kv.validKey(op.Key) {
				res.Err = ErrWrongGroup
			} else if kv.lastAck[key2shard(op.Key)][op.ClientID] < op.OpID {
				kv.db[key2shard(op.Key)][op.Key] = op.Val
				kv.lastAck[key2shard(op.Key)][op.ClientID] = op.OpID
			} else {
				res.Err = ErrWrongLeader
			}
		} else if op.Cmd == "Append" {
			if !kv.validKey(op.Key) {
				res.Err = ErrWrongGroup
			} else if kv.lastAck[key2shard(op.Key)][op.ClientID] < op.OpID {
				kv.db[key2shard(op.Key)][op.Key] += op.Val
				kv.lastAck[key2shard(op.Key)][op.ClientID] = op.OpID
			} else {
				res.Err = ErrWrongLeader
			}
		} else if op.Cmd == "Get" {
			_, ok := kv.db[key2shard(op.Key)][op.Key]
			kv.lastAck[key2shard(op.Key)][op.ClientID] = op.OpID
			if !ok {
				res.Err = ErrNoKey
			}
		} else if op.Cmd == "Reconfigure" {

			if op.Config.Num != kv.config.Num+1 {
				kv.mu.Unlock()
				continue
			}

			for i := 0; i < shardmaster.NShards; i++ {
				for k, v := range op.DB[i] {
					kv.db[i][k] = v
				}
				for k, v := range op.LastAck[i] {
					kv.lastAck[i][k] = v
				}
			}
			kv.config = op.Config
		}

		res.Cmd = op.Cmd
		res.Key = op.Key
		res.Val = kv.db[key2shard(op.Key)][op.Key]
		kv.lastRaftCommandIndex = msg.CommandIndex
		if _, ok := kv.resultCh[msg.CommandIndex]; ok {
			kv.resultCh[msg.CommandIndex] <- res
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) takeSnapshot() {
	currentCommandIndex := 0
	for !kv.killed() {
		kv.mu.Lock()
		if kv.maxraftstate > 0 && kv.rf.GetRaftStateSize() > kv.maxraftstate && kv.lastRaftCommandIndex > currentCommandIndex { // threshold??
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.db)
			e.Encode(kv.lastAck)
			e.Encode(kv.config)
			currentCommandIndex = kv.lastRaftCommandIndex
			if !kv.rf.StoreSnapshot(w.Bytes(), kv.lastRaftCommandIndex) { // we need to store the command Index
			}
		}

		kv.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
}

// thread for getConfig
func (kv *ShardKV) reconfigure() {
	for !kv.killed() {
		// check leader
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.mu.Lock()

		nextConfig := kv.sm.Query(kv.config.Num + 1)
		currConfig := kv.sm.Query(kv.config.Num)
		gid := kv.gid
		kv.mu.Unlock()
		var wg sync.WaitGroup
		var l sync.Mutex
		var entry Op
		entry.Cmd = "Reconfigure"
		entry.Config = nextConfig
		for i := 0; i < shardmaster.NShards; i++ {
			entry.DB[i] = make(map[string]string)
			entry.LastAck[i] = make(map[int64]int64)
		}

		ok := true

		if nextConfig.Num == currConfig.Num+1 {
			for i := 0; i < shardmaster.NShards; i++ {
				if nextConfig.Shards[i] != currConfig.Shards[i] && nextConfig.Shards[i] == gid && currConfig.Shards[i] != 0 {
					wg.Add(1)
					go func(i int) {
						var args GetShardsArgs
						var reply GetShardsReply
						args.ConfigNum = nextConfig.Num
						args.Gid = gid
						args.Shard = i
						if kv.sendGetShards(i, currConfig, &args, &reply) {
							// add it to entry for storing in raft
							l.Lock()
							entry.DB[i] = reply.DB[i]
							entry.LastAck[i] = reply.LastAck[i]
							l.Unlock()
						} else {
							l.Lock()
							ok = false
							l.Unlock()
						}

						wg.Done()
					}(i)
				}
			}
		} else {
			ok = false
		}

		wg.Wait()
		if ok {
			var res Result
			kv.sendRaftMsg(entry, &res)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) sendGetShards(shard int, currConfig shardmaster.Config, args *GetShardsArgs, reply *GetShardsReply) bool {
	txGid := currConfig.Shards[shard]
	servers := currConfig.Groups[txGid]
	// try each server for the shard.
	for si := 0; si < len(servers); si++ {
		srv := kv.make_end(servers[si])
		ok := srv.Call("ShardKV.GetShards", args, reply)
		if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
			// fmt.Println("sendGetShards true")
			return true // think for no key
		}
		if ok && (reply.Err == ErrWrongGroup) {
			break
		}
		// ... not ok, or ErrWrongLeader
	}
	// wrong group all wrong leader
	return false
}

func (kv *ShardKV) GetShards(args *GetShardsArgs, reply *GetShardsReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if args.ConfigNum > (kv.config.Num + 1) {
		reply.Err = ErrWrongLeader
		return
	}
	// should we transfer multiple shards to same gid??
	// check if the args are correct??
	for i := 0; i < shardmaster.NShards; i++ {
		reply.DB[i] = make(map[string]string)
		reply.LastAck[i] = make(map[int64]int64)
	}

	// copy the shards in args
	for k, v := range kv.db[args.Shard] {
		reply.DB[args.Shard][k] = v
	}

	for k, v := range kv.lastAck[args.Shard] {
		reply.LastAck[args.Shard][k] = v
	}
	// remove the shard from servicing
	kv.config.Shards[args.Shard] = args.Gid

	reply.Err = OK
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

	kv.applyCh = make(chan raft.ApplyMsg, 10)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.db[i] = make(map[string]string)
		kv.lastAck[i] = make(map[int64]int64)
	}
	kv.resultCh = make(map[int]chan Result)

	go kv.receiveRaftMsg()
	go kv.takeSnapshot()
	go kv.reconfigure()
	return kv
}
