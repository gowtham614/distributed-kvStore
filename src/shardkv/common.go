package shardkv

import "../shardmaster"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClientID int64
	OpID     int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	ClientID int64
	OpID     int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type GetShardsArgs struct {
	Shard     int
	ConfigNum int
	Gid       int
}

type GetShardsReply struct {
	Err     Err
	DB      [shardmaster.NShards]map[string]string
	LastAck [shardmaster.NShards]map[int64]int64
}
