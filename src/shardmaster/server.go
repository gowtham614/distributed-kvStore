package shardmaster

import (
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	// Your data here.

	configs              []Config // indexed by config num
	resultCh             map[int]chan Result
	lastAck              map[int64]int64
	lastRaftCommandIndex int // used for snapshot
}

type Op struct {
	// Your data here.
	Cmd      string
	ClientID int64
	OpID     int64
	// Join args
	Servers map[int][]string
	// Leave args
	GIDs []int
	// Move args
	Shard int
	GID   int
	// Query args
	Num int
}

type Result struct {
	WrongLeader bool
	Err         Err
	Cmd         string
	ClientID    int64
	OpID        int64
	Config      Config
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// fmt.Println("sm ", sm.me, "Join ", args.ClientID, args.OpID, "Servers =", args.Servers)
	// Your code here.
	var op Op
	op.Cmd = "Join"
	op.ClientID = args.ClientID
	op.OpID = args.OpID
	op.Servers = args.Servers

	var res Result
	// check for last ack, duplicate
	sm.sendRaftMsg(op, &res)
	reply.WrongLeader = res.WrongLeader
	reply.Err = res.Err
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// fmt.Println("sm ", sm.me, "Leave ", args.ClientID, args.OpID, "GIDs =", args.GIDs)
	// Your code here.
	var op Op
	op.Cmd = "Leave"
	op.ClientID = args.ClientID
	op.OpID = args.OpID
	op.GIDs = args.GIDs

	var res Result
	// check for last ack, duplicate
	sm.sendRaftMsg(op, &res)
	reply.WrongLeader = res.WrongLeader
	reply.Err = res.Err
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// fmt.Println("sm ", sm.me, "Move ", args.ClientID, args.OpID, "Shard =", args.Shard, "GID=", args.GID)
	// Your code here.
	var op Op
	op.Cmd = "Move"
	op.ClientID = args.ClientID
	op.OpID = args.OpID
	op.Shard = args.Shard
	op.GID = args.GID

	var res Result
	// check for last ack, duplicate
	sm.sendRaftMsg(op, &res)
	reply.WrongLeader = res.WrongLeader
	reply.Err = res.Err
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// fmt.Println("sm ", sm.me, "Query ", args.ClientID, args.OpID, "Num =", args.Num)
	// Your code here.
	var op Op
	op.Cmd = "Query"
	op.ClientID = args.ClientID
	op.OpID = args.OpID
	op.Num = args.Num

	var res Result
	// check for last ack, duplicate
	sm.sendRaftMsg(op, &res)
	reply.WrongLeader = res.WrongLeader
	reply.Err = res.Err
	reply.Config = res.Config
	// fmt.Println("sm ", sm.me, "Query reply", args.ClientID, args.OpID, "Num =", args.Num, "config =", res.Config)
}

func (sm *ShardMaster) sendRaftMsg(op Op, res *Result) {
	// check for last ack, duplicate??
	idx, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		res.WrongLeader = true
		return
	}
	sm.mu.Lock()

	if _, ok := sm.resultCh[idx]; !ok {
		sm.resultCh[idx] = make(chan Result, 1)
	}
	p := sm.resultCh[idx]
	sm.mu.Unlock()
	select {
	case result := <-p:
		if op.Cmd != result.Cmd || op.ClientID != result.ClientID || op.OpID != result.OpID {
			res.WrongLeader = true
			return
		}

		res.WrongLeader = false
		res.Err = result.Err
		res.Cmd = result.Cmd
		res.Config = result.Config

	case <-time.After(2000 * time.Millisecond):
		res.WrongLeader = true
	}
}

// thread for receiving all raft msg
func (sm *ShardMaster) receiveRaftMsg() {
	for !sm.killed() {
		msg := <-sm.applyCh
		sm.mu.Lock()

		op := msg.Command.(Op)

		var res Result
		res.Cmd = op.Cmd
		res.ClientID = op.ClientID
		res.OpID = op.OpID

		id := sm.lastAck[op.ClientID]
		duplicate := false
		if id >= op.OpID {
			// fmt.Println("sm ", sm.me, "receiveRaftMsg() duplicate received")
			duplicate = true
		}

		switch op.Cmd {
		case "Join":
			if !duplicate {
				sm.applyJoin(op)
			}
		case "Leave":
			if !duplicate {
				sm.applyLeave(op)
			}
		case "Move":
			if !duplicate {
				c := sm.makeNewConfig()
				c.Shards[op.Shard] = op.GID
				sm.configs = append(sm.configs, c)
			}
		case "Query":
			if op.Num == -1 || op.Num > sm.configs[len(sm.configs)-1].Num {
				res.Config = sm.configs[len(sm.configs)-1]
			} else {
				res.Config = sm.configs[op.Num]
			}
		}

		if !duplicate {
			sm.lastAck[op.ClientID] = op.OpID
		}

		res.WrongLeader = false
		res.Err = OK
		if _, ok := sm.resultCh[msg.CommandIndex]; ok {
			sm.resultCh[msg.CommandIndex] <- res
		}
		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) applyJoin(op Op) {
	c := sm.makeNewConfig()
	for gid, servers := range op.Servers {
		c.Groups[gid] = servers
	}
	// fmt.Println("sm ", sm.me, "applyJoin before balance config =", c)
	sm.rebalance(&c)
	sm.configs = append(sm.configs, c)
}

func (sm *ShardMaster) applyLeave(op Op) {
	// fmt.Println("\nsm ", sm.me, "applyLeave", op.ClientID, op.OpID, "GIDs =", op.GIDs)
	c := sm.makeNewConfig()
	for _, delGid := range op.GIDs {
		for shard, gid := range c.Shards {
			if gid == delGid {
				c.Shards[shard] = 0
			}
		}
		delete(c.Groups, delGid)
	}
	// fmt.Println("\nsm ", sm.me, "applyLeave", op.ClientID, op.OpID, "before balance config =", c)
	sm.rebalance(&c)
	sm.configs = append(sm.configs, c)
}

func (sm *ShardMaster) rebalance(c *Config) {
	if len(c.Groups) == 0 {
		return
	}
	GIDArray := make([]int, len(c.Groups))
	i := 0
	for gid := range c.Groups {
		GIDArray[i] = gid
		i++
	}
	// fill shards
	i = 0
	for j := 0; j < NShards; j++ {
		if c.Shards[j] == 0 {
			pos := i % len(GIDArray)
			c.Shards[j] = GIDArray[pos]
			i++
		}
	}

	// rebalance shards
	meanShards := NShards / len(c.Groups)
	GID2Shard := map[int][]int{} // gid -> shard
	for shard, gid := range c.Shards {
		if _, ok := GID2Shard[gid]; !ok {
			GID2Shard[gid] = make([]int, 0)
		}
		GID2Shard[gid] = append(GID2Shard[gid], shard)
	}

	// fmt.Println("sm ", sm.me, "rebalance GID2Shard =", GID2Shard, "meanShards =", meanShards)

	for gid := range c.Groups {
		for len(GID2Shard[gid]) < meanShards {
			for srcGid := range c.Groups {
				if len(GID2Shard[srcGid]) > meanShards && gid != srcGid {
					last := len(GID2Shard[srcGid]) - 1
					shard := GID2Shard[srcGid][last]
					GID2Shard[srcGid] = GID2Shard[srcGid][:last]
					GID2Shard[gid] = append(GID2Shard[gid], shard)
					c.Shards[shard] = gid
				}
			}
		}
	}
	// fmt.Println("\n\nsm ", sm.me, "rebalance latest config =", c)
}

// calling function holds lock
func (sm *ShardMaster) makeNewConfig() Config {
	var c Config
	lastConfig := sm.configs[len(sm.configs)-1]
	c.Num = lastConfig.Num + 1
	c.Shards = lastConfig.Shards
	c.Groups = map[int][]string{}
	for gid, servers := range lastConfig.Groups {
		c.Groups[gid] = servers
	}
	return c
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.rf.Kill()
	// Your code here, if desired.
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg, 10)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.resultCh = make(map[int]chan Result)
	sm.lastAck = make(map[int64]int64)

	go sm.receiveRaftMsg()

	return sm
}
