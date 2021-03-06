package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Idx     int // idx of this entry
	Command interface{}
	Term    int // term when entry was received by leader, set to 1 initiailly
}

const (
	STATE_CANDIDATE = iota
	STATE_FOLLOWER
	STATE_LEADER
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// we need state to know about this server
	// candidate, leader, follower
	state           int       // one of the three state
	electionTimeout time.Time // when current time crosses this, start election

	// persistent state
	currentTerm int        // latest term
	votedFor    int        //
	log         []LogEntry // should be logType struct

	// volatile leader state
	nextIndex  []int // can be a fixed array of #peers, set 0 initially
	matchIndex []int // can be a fixed array of #peers, set 0 initially

	// volatile state
	commitIndex int
	lastApplied int
	voteCount   int
	applyCh     chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	debugCount         int  // remove this when working
	heartBeatImmediate bool // to send heartbear immediately
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	isleader = (rf.state == STATE_LEADER)
	term = rf.currentTerm
	// fmt.Println("Election timeout - timenow", rf.me, time.Now().Sub(rf.electionTimeout))
	rf.mu.Unlock()
	// fmt.Println("GetState ", rf.me, "leader = ", isleader, " term =", term)
	return term, isleader
}

func (rf *Raft) getStartIdx() int { // assumes the caller has lock
	return rf.log[0].Idx
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//

func (rf *Raft) encodedRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return w.Bytes()
}

func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.encodedRaftState())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.currentTerm = 0
		rf.votedFor = -1
		rf.log = append(rf.log, LogEntry{Term: 0, Idx: 0})
		return
	}
	// var snapshot []byte
	snapshot := rf.persister.ReadSnapshot()
	if snapshot != nil || len(snapshot) > 0 {

		sr := bytes.NewBuffer(snapshot)
		sd := labgob.NewDecoder(sr)
		var lastIdx int
		var lastTerm int
		sd.Decode(&lastIdx)
		sd.Decode(&lastTerm)
		// println("raft", rf.me, "readPersist remaining bytes ", sr.Bytes())
		// rf.commitIndex = args.LastIncludedIdx
		// rf.lastApplied = args.LastIncludedIdx
		msg := ApplyMsg{CommandValid: false, Command: sr.Bytes(), CommandIndex: lastIdx}
		rf.applyCh <- msg
		println("raft", rf.me, "readPersist sent message")
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	rf.commitIndex = rf.log[0].Idx
	rf.lastApplied = rf.log[0].Idx
	println("raft", rf.me, "readPersist done")
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) trimRaftLog(lastIncludedIdx int, lastIncludedTerm int) { // assumes lock held by caller
	newLog := make([]LogEntry, 0)
	newLog = append(newLog, LogEntry{Idx: lastIncludedIdx, Term: lastIncludedTerm})

	if lastIncludedIdx < rf.log[len(rf.log)-1].Idx && rf.log[lastIncludedIdx-rf.getStartIdx()].Idx == lastIncludedIdx &&
		rf.log[lastIncludedIdx-rf.getStartIdx()].Term == lastIncludedTerm {
		newLog = append(newLog, rf.log[lastIncludedIdx-rf.getStartIdx()+1:]...)
	}

	// for i := len(rf.log) - 1; i >= 0; i-- {
	// 	if rf.log[i].Idx < lastIncludedIdx {
	// 		break
	// 	}
	// 	if rf.log[i].Idx == lastIncludedIdx && rf.log[i].Term == lastIncludedTerm {
	// 		newLog = append(newLog, rf.log[i+1:]...)
	// 		break
	// 	}
	// }

	rf.log = newLog
}

func (rf *Raft) StoreSnapshot(kvSnapshot []byte, lastIdx int) bool {
	// fmt.Println("raft", rf.me, "store snapshot ", rf.log, lastIdx)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIdx <= rf.getStartIdx() || lastIdx > rf.log[len(rf.log)-1].Idx {
		// can't trim log since index is invalid
		return false
	}
	return rf.storeSnapshot(kvSnapshot, lastIdx, rf.log[lastIdx-rf.getStartIdx()].Term)
}

func (rf *Raft) storeSnapshot(kvSnapshot []byte, lastIdx int, lastTerm int) bool { // lock held by caller
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(lastIdx) // this needs to change, idx should be stored with log
	e.Encode(lastTerm)
	fmt.Println("raft", rf.me, "store snapshot ", lastIdx)
	snapshot := append(w.Bytes(), kvSnapshot...)
	// trim log
	rf.trimRaftLog(lastIdx, lastTerm)
	rf.persister.SaveStateAndSnapshot(rf.encodedRaftState(), snapshot)

	return true
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) getElectionTimeout() time.Time {
	return time.Now().Add(time.Millisecond * time.Duration(rand.Intn(30)*10+200)) // 300 - 600 ms, with 300ms as step size
}

func (rf *Raft) startElection() {
	// fmt.Println("Election() ", rf.me)
	for !rf.killed() {
		// fmt.Println("Election timeout - timenow", rf.me, time.Now().Sub(rf.electionTimeout))
		rf.mu.Lock()
		isTimeout := (time.Now().Sub(rf.electionTimeout).Microseconds() > 0)
		// rf.mu.Unlock()
		if isTimeout {
			rf.debugCount++
			fmt.Println("raft", rf.me, "started Election ", rf.electionTimeout, rf.debugCount)
			var args RequestVoteArgs

			// rf.mu.Lock()
			rf.currentTerm = rf.currentTerm + 1
			rf.votedFor = rf.me
			rf.electionTimeout = rf.getElectionTimeout()
			rf.state = STATE_CANDIDATE
			rf.persist()

			// send request vote to others

			args.CandidateId = rf.me
			// need to have at least 1 log entry
			args.LastLogTerm = rf.log[len(rf.log)-1].Term
			args.LastLogIndex = rf.log[len(rf.log)-1].Idx
			args.Term = rf.currentTerm
			rf.voteCount = 1
			// fmt.Println("voting myself", rf.me)
			// rf.mu.Unlock()
			for i := 0; (i < len(rf.peers)) && (rf.state == STATE_CANDIDATE); i++ { // this should be allowed only when candidate
				// fmt.Println("start election", rf.me)
				if i == rf.me {
					continue
				}

				go func(x int) {
					var reply RequestVoteReply
					// fmt.Println("sending req vote", rf.me, x)
					if !rf.sendRequestVote(x, &args, &reply) {
						return
						// fmt.Println("waiting for request vote to get reply ", rf.me, x)
					}
					// *** we need to check for the reply coming for older term irrelevant replies and throw it away
					rf.mu.Lock()
					defer rf.mu.Unlock()
					// count vote check for candidate legitamacy
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						fmt.Println("raft", rf.me, "start() became follower")
						rf.state = STATE_FOLLOWER
						rf.persist()
						return
					}

					if args.Term != rf.currentTerm { // some older reply coming
						return
					}

					if reply.VoteGranted {
						rf.voteCount++
						fmt.Println("raft", rf.me, "vote granted", x)
						if rf.voteCount > (len(rf.peers) / 2) {
							rf.state = STATE_LEADER
							// we should send empty append entries
							fmt.Println("raft", rf.me, "became leader")
							for j := 0; j < len(rf.peers); j++ {
								rf.nextIndex[j] = rf.log[len(rf.log)-1].Idx + 1
								rf.matchIndex[j] = 0
							}
							rf.heartBeatImmediate = true
							return
						}
					} else {
						// not got vote, check and change to follower may be
						fmt.Println("raft", rf.me, "vote not granted", x)
					}
					return
				}(i)
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond)

	}
}

func (rf *Raft) heartBeat() {
	lastHeartBeatTime := time.Now()
	for !rf.killed() {
		rf.mu.Lock()
		isLeader := (rf.state == STATE_LEADER)
		if isLeader {
			rf.electionTimeout = rf.getElectionTimeout()
			if (time.Now().Sub(lastHeartBeatTime).Milliseconds() > 60) || rf.heartBeatImmediate { // send after 60 ms

				fmt.Println("raft", rf.me, "sending heart beat", rf.debugCount, "immediate", rf.heartBeatImmediate)
				rf.heartBeatImmediate = false
				rf.debugCount++

				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					if rf.state != STATE_LEADER {
						break
					}

					if rf.nextIndex[i] <= rf.getStartIdx() {
						// install snapshot
						var args InstallSnapshotArgs
						args.Term = rf.currentTerm
						args.LeaderId = rf.me
						snapshot := rf.persister.ReadSnapshot()
						sr := bytes.NewBuffer(snapshot)
						sd := labgob.NewDecoder(sr)
						sd.Decode(&args.LastIncludedIdx)
						sd.Decode(&args.LastIncludedTerm)
						args.Data = sr.Bytes()

						go func(i int, args InstallSnapshotArgs) {
							var reply InstallSnapshotReply
							if !rf.sendInstallSnapshot(i, &args, &reply) {
								return
							}
							rf.mu.Lock()
							defer rf.mu.Unlock()
							if args.Term != rf.currentTerm || rf.state != STATE_LEADER { // some older reply coming
								return
							}
							if reply.Success == false {
								// fmt.Println("sendAppendEntries reply failed", rf.me, x)
								if reply.Term > rf.currentTerm {
									fmt.Println("raft", rf.me, "heartbeat() became follower")
									rf.state = STATE_FOLLOWER
									rf.currentTerm = reply.Term
									rf.persist()
									return
								}
							} else {
								rf.nextIndex[i] = rf.getStartIdx() + 1
								rf.matchIndex[i] = rf.getStartIdx()
							}
							return
						}(i, args)
						continue
					}
					// regular send append entries
					var tempargs AppendEntriesArgs
					idx := max(rf.nextIndex[i], rf.getStartIdx()+1)
					tempargs.PrevLogIndex = idx - 1
					fmt.Println("raft", rf.me, "heart beat ", "prevLogIdx", tempargs.PrevLogIndex, "sidx ", rf.getStartIdx(), " cid", i)
					tempargs.PrevLogTerm = rf.log[tempargs.PrevLogIndex-rf.getStartIdx()].Term
					buffCopy := make([]LogEntry, len(rf.log[idx-rf.getStartIdx():]))
					copy(buffCopy, rf.log[idx-rf.getStartIdx():])
					tempargs.Entries = buffCopy
					tempargs.LeaderCommit = rf.commitIndex
					tempargs.LeaderId = rf.me
					tempargs.DebugCount = rf.debugCount

					tempargs.Term = rf.currentTerm

					go func(x int, args AppendEntriesArgs) {

						var reply AppendEntriesReply

						// fmt.Println("entries", rf.me, " i =", x, "prev log idx =", args.PrevLogIndex, args.Entries)

						rf.mu.Lock()
						fmt.Println("raft", rf.me, "count", tempargs.DebugCount, " sendAppendEntries", x, "next index[", x, "]", rf.nextIndex[x], "prev index", args.PrevLogIndex)
						rf.mu.Unlock()
						if !rf.sendAppendEntries(x, &args, &reply) { // may this should be if, since some can offline and cant be reached
							rf.mu.Lock()
							fmt.Println("raft", rf.me, "count", tempargs.DebugCount, " sendAppendEntries failed to send", x, "next index[", x, "]", rf.nextIndex[x], "prev index", args.PrevLogIndex)
							rf.mu.Unlock()
							return
						}
						// *** we need to check for the reply coming for older term irrelevant replies and throw it away

						rf.mu.Lock()
						defer rf.mu.Unlock()
						if args.Term != rf.currentTerm || rf.state != STATE_LEADER { // some older reply coming
							return
						}

						if reply.Success == false {
							fmt.Println("raft", rf.me, "sendAppendEntries reply failed", x)
							if reply.Term > rf.currentTerm {
								fmt.Println("raft", rf.me, "heartbeat() became follower")
								rf.state = STATE_FOLLOWER
								rf.currentTerm = reply.Term
								rf.persist()
								return
							}

							rf.nextIndex[x] = min(reply.NextIndex, rf.log[len(rf.log)-1].Idx+1)
							rf.heartBeatImmediate = true
							fmt.Println("raft", rf.me, "count", tempargs.DebugCount, " sendAppendEntries", " reducing next index[", x, "]", rf.nextIndex[x])
							return
						} else {
							rf.nextIndex[x] = min(reply.NextIndex, rf.log[len(rf.log)-1].Idx+1)
							rf.matchIndex[x] = reply.NextIndex - 1
							fmt.Println("raft", rf.me, "success increasing match index[", x, "]", rf.matchIndex[x])
						}

						count := 1
						for j := 0; j < len(rf.peers); j++ {
							if rf.matchIndex[j] >= rf.matchIndex[x] {
								count++
								if count > (len(rf.peers) / 2) {
									// ** what we the old commit index comes later in time
									rf.commitIndex = rf.matchIndex[x]
									fmt.Println("raft", rf.me, "heartbeat commit index", rf.commitIndex)
								}
							}

						}
						return
					}(i, tempargs)
				}
				lastHeartBeatTime = time.Now()
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	fmt.Println("raft", rf.me, "RequestVote, myid =", "cid = ", args.CandidateId)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		fmt.Println("raft", rf.me, "RequestVote, myid =", "cid = ", args.CandidateId, " term less my term ", rf.currentTerm, "candidate term", args.Term)
		return
	}
	if args.Term > rf.currentTerm {
		// become follower and update current term
		fmt.Println("raft", rf.me, "requestvote() became follower")
		rf.state = STATE_FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	// candidate’s log is at least as up-to-date as receiver’s log, grant vote

	term := rf.log[len(rf.log)-1].Term
	idx := rf.log[len(rf.log)-1].Idx
	uptoDate := (args.LastLogTerm > term || (args.LastLogTerm == term && args.LastLogIndex >= idx))
	if ((rf.votedFor == -1) || rf.votedFor == args.CandidateId) && uptoDate {
		fmt.Println("raft", rf.me, "RequestVote success, myid =", "cid = ", args.CandidateId, "error log upto date ", uptoDate,
			"my term", term, "my idx = ", idx, "cad term ", args.LastLogTerm, "cad index", args.LastLogIndex)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		return
	}
	fmt.Println("raft", rf.me, "RequestVote, myid =", "cid = ", args.CandidateId, " voted for", rf.votedFor, "log upto date ", uptoDate)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int // leader commit index
	DebugCount   int
}

type AppendEntriesReply struct {
	Term      int
	NextIndex int
	Success   bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	idx := rf.log[len(rf.log)-1].Idx
	reply.Success = false
	reply.Term = rf.currentTerm
	reply.NextIndex = idx + 1
	if args.Term < rf.currentTerm {
		fmt.Println("raft", rf.me, "count ", args.DebugCount, "fail Append entries", "cid = ", args.LeaderId, "less term my term ", rf.currentTerm, "candidate term", args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		fmt.Println("raft", rf.me, "count ", args.DebugCount, "appendentries() became follower")
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
	}

	rf.electionTimeout = rf.getElectionTimeout()
	sIdx := rf.getStartIdx()

	// have doubt about this
	if args.PrevLogIndex > idx {
		fmt.Println("raft", rf.me, "count ", args.DebugCount, "fail idx greater Append entries", "cid = ", args.LeaderId, "log idx incorrect , equal term",
			"prev log index = ", args.PrevLogIndex, " idx = ", idx)
		// fmt.Println("count ", args.DebugCount, "next index", reply.NextIndex) //, rf.log)
		return
	}

	if (args.PrevLogIndex - sIdx) < 0 {
		return
	}
	if args.PrevLogTerm != rf.log[args.PrevLogIndex-sIdx].Term {
		fmt.Println("raft", rf.me, "count ", args.DebugCount, "fail Append entries", "cid = ", args.LeaderId, "log idx incorrect , equal term",
			"prev log index = ", args.PrevLogIndex, " idx = ", idx, "term incorrect")
		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			if (i - sIdx) < 0 {
				return
			}
			if rf.log[i-sIdx].Term != rf.log[args.PrevLogIndex-sIdx].Term {
				reply.NextIndex = i + 1
				// fmt.Println("count ", args.DebugCount, rf.me, "cid = ", args.LeaderId, "next index", reply.NextIndex)
				return
			}
		}
		return
	}

	currIdx := args.PrevLogIndex + 1
	// fmt.Println("Append entries setting to follower ", rf.me)
	reply.Success = true
	if rf.state != STATE_FOLLOWER {
		// fmt.Println("appendentries() last became follower", rf.me)
	}
	rf.state = STATE_FOLLOWER
	rf.currentTerm = args.Term

	if currIdx <= idx {
		// if rf.commitIndex > currIdx {
		// 	fmt.Println("count ", args.DebugCount, "append entries ", rf.me, "curridx", currIdx, "commit index", rf.commitIndex, "failed return")
		// 	return
		// }
		// this is risky we need to check for prev appendEnties which are delayed, check all the entries if its present dont remove
		// when the first index fails remove the succeeding entries

		newLength := args.PrevLogIndex + len(args.Entries) + 1
		if newLength <= rf.log[len(rf.log)-1].Idx+1 {
			j := 0
			for i := args.PrevLogIndex + 1; i < newLength && i < rf.log[len(rf.log)-1].Idx+1; i++ {
				if (rf.log[i-sIdx].Command != args.Entries[j].Command) || (rf.log[i-sIdx].Term != args.Entries[j].Term) {
					rf.log = rf.log[:i-sIdx]
					rf.log = append(rf.log, args.Entries[j:]...)
					// fmt.Println("count ", args.DebugCount, "sucess append entries", rf.me, "cid = ", args.LeaderId, "deleted logs", "enteis", args.Entries, "previndex", args.PrevLogIndex,
					// 	"idx", idx, "next index", reply.NextIndex)
					reply.NextIndex = rf.log[len(rf.log)-1].Idx + 1
					return
				}
				j++
			}
			reply.NextIndex = rf.log[len(rf.log)-1].Idx + 1
			// fmt.Println("count ", args.DebugCount, "sucess append entries", rf.me, "cid = ", args.LeaderId, "some old rpc", "previndex", args.PrevLogIndex,
			// 	"idx", idx, "next index", reply.NextIndex)
			return
		}

		rf.log = rf.log[:currIdx-sIdx]
		rf.log = append(rf.log, args.Entries...)
		reply.NextIndex = rf.log[len(rf.log)-1].Idx + 1
		// fmt.Println("count ", args.DebugCount, "sucess append entries", rf.me, "cid = ", args.LeaderId, "deleted logs", "enteis", args.Entries, "previndex", args.PrevLogIndex,
		// 	"idx", idx, "next index", reply.NextIndex)
		return
	} else {
		rf.log = append(rf.log, args.Entries...)
		reply.NextIndex = rf.log[len(rf.log)-1].Idx + 1
		// fmt.Println("count ", args.DebugCount, "success append entries", rf.me, "cid = ", args.LeaderId, "appended logs", "enteis", args.Entries, "previndex", args.PrevLogIndex,
		// 	"idx", idx, "next index", reply.NextIndex)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.log[len(rf.log)-1].Idx)))
		// fmt.Println("count ", args.DebugCount, "AppendEntries ", rf.me, "commit index", rf.commitIndex, rf.log, "entries", args.Entries)
	}

	if len(args.Entries) > 0 {
		fmt.Println("raft", rf.me, "count ", args.DebugCount, "AppendEntries logs", rf.log, "entries", args.Entries, args.PrevLogIndex)
	}
	fmt.Println("raft", rf.me, "count ", args.DebugCount, "success Append entries", "cid = ", args.LeaderId)
	return
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludedIdx  int
	LastIncludedTerm int
	Data             []byte // snapshot
}

type InstallSnapshotReply struct {
	Term    int
	Success bool
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	fmt.Println("raft", rf.me, "installSnapshot cid", args.LeaderId)
	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		fmt.Println("raft", rf.me, "installSnapshot less term ", "cid", args.LeaderId)
		return
	}
	if args.Term > rf.currentTerm {
		rf.state = STATE_FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	reply.Success = true
	reply.Term = rf.currentTerm

	if args.LastIncludedIdx > rf.commitIndex {

		rf.storeSnapshot(args.Data, args.LastIncludedIdx, args.LastIncludedTerm)
		rf.commitIndex = args.LastIncludedIdx
		rf.lastApplied = args.LastIncludedIdx
		fmt.Println("raft", rf.me, "install snapshot sending kv send cid", args.LeaderId)
		msg := ApplyMsg{CommandValid: false, Command: args.Data, CommandIndex: args.LastIncludedIdx}
		rf.applyCh <- msg
	}

	//  If existing log entry has same index and term as snapshot’s
	// last included entry, retain log entries following it and reply

	//  Discard the entire log

	//  Reset state machine using snapshot contents (and load
	// snapshot’s cluster configuration)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	fmt.Println("raft", rf.me, "start()", command)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Println("raft", rf.me, "start() got lock", command)
	index := -1
	term := -1
	isLeader := rf.state == STATE_LEADER

	if !isLeader {
		// fmt.Println("Start() not leader", rf.me, time.Now().Sub(rf.electionTimeout))
		return index, term, isLeader
	}

	term = rf.currentTerm
	index = rf.log[len(rf.log)-1].Idx + 1
	m := LogEntry{Term: rf.currentTerm, Command: command, Idx: index}
	rf.log = append(rf.log, m)
	rf.persist()
	fmt.Println("raft", rf.me, "start()", m)

	rf.heartBeatImmediate = true
	return index, term, isLeader
}

func (rf *Raft) applyLog() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {

			if (rf.lastApplied + 1) >= rf.log[len(rf.log)-1].Idx+1 {
				break
			}
			rf.lastApplied++
			sIdx := rf.log[0].Idx
			m := ApplyMsg{true, rf.log[rf.lastApplied-sIdx].Command, rf.lastApplied}
			fmt.Println("raft", rf.me, "apply log()", "waiting", m)
			// select {
			// case rf.applyCh <- m: // Put in the channel unless it is full
			// default:
			// 	fmt.Println("raft", rf.me, "Channel full. Discarding value")
			// 	continue
			// }
			rf.mu.Unlock()
			rf.applyCh <- m

			fmt.Println("raft", rf.me, "apply log()", "last applied", rf.lastApplied, m)
			time.Sleep(time.Millisecond)
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	// fmt.Println("killed ", rf.me)
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	fmt.Println("raft", me, "restart")
	// fmt.Println("restart --- ", me)
	rf := &Raft{}
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = STATE_FOLLOWER
	rf.applyCh = applyCh
	rf.debugCount = 0

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	fmt.Println("raft", rf.me, "readPersist done")
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.electionTimeout = rf.getElectionTimeout()
	rf.mu.Unlock()
	go rf.startElection()
	go rf.heartBeat()
	go rf.applyLog()

	return rf
}
