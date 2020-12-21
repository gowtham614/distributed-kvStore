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

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	rf.persister.SaveRaftState(w.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
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
		rf.mu.Unlock()
		if isTimeout {
			rf.debugCount++
			// fmt.Println("started Election ", rf.me, rf.electionTimeout, rf.debugCount)
			var args RequestVoteArgs

			rf.mu.Lock()
			rf.currentTerm = rf.currentTerm + 1
			rf.votedFor = rf.me
			rf.electionTimeout = rf.getElectionTimeout()
			rf.state = STATE_CANDIDATE
			rf.persist()

			// send request vote to others

			args.CandidateId = rf.me
			// need to have at least 1 log entry
			args.LastLogTerm = rf.log[len(rf.log)-1].Term
			args.LastLogIndex = len(rf.log) - 1
			args.Term = rf.currentTerm
			rf.voteCount = 1
			// fmt.Println("voting myself", rf.me)
			rf.mu.Unlock()
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
						// fmt.Println("start() became follower", rf.me)
						rf.state = STATE_FOLLOWER
						rf.persist()
						return
					}

					if args.Term != rf.currentTerm { // some older reply coming
						return
					}

					if reply.VoteGranted {
						rf.voteCount++
						// fmt.Println("vote granted", rf.me, x)
						if rf.voteCount > (len(rf.peers) / 2) {
							rf.state = STATE_LEADER
							// we should send empty append entries
							// fmt.Println("became leader", rf.me)
							for j := 0; j < len(rf.peers); j++ {
								rf.nextIndex[j] = len(rf.log)
								rf.matchIndex[j] = len(rf.log) - 1
							}
							rf.heartBeatImmediate = true
							return
						}
					} else {
						// not got vote, check and change to follower may be
						// fmt.Println("vote not granted", rf.me, x)
					}
					return
				}(i)
			}
		} else {
			time.Sleep(time.Millisecond)
		}
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

				// fmt.Println("sending heart beat", rf.me, rf.debugCount, "immediate", rf.heartBeatImmediate)
				rf.heartBeatImmediate = false
				rf.debugCount++

				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					if rf.state != STATE_LEADER {
						break
					}

					var tempargs AppendEntriesArgs

					idx := max(rf.matchIndex[i], 0)
					tempargs.PrevLogIndex = idx
					tempargs.PrevLogTerm = rf.log[tempargs.PrevLogIndex].Term
					tempargs.Entries = rf.log[idx+1:]
					tempargs.LeaderCommit = rf.commitIndex
					tempargs.LeaderId = rf.me
					tempargs.DebugCount = rf.debugCount

					tempargs.Term = rf.currentTerm

					go func(x int, args AppendEntriesArgs) {

						var reply AppendEntriesReply

						// fmt.Println("entries", rf.me, " i =", x, "prev log idx =", args.PrevLogIndex, args.Entries)

						// fmt.Println("count", rf.debugCount, " sendAppendEntries", rf.me, x, "match index[", x, "]", rf.matchIndex[x], "prev index", args.PrevLogIndex)

						if !rf.sendAppendEntries(x, &args, &reply) { // may this should be if, since some can offline and cant be reached
							return
						}
						// *** we need to check for the reply coming for older term irrelevant replies and throw it away

						rf.mu.Lock()
						defer rf.mu.Unlock()
						if args.Term != rf.currentTerm || rf.state != STATE_LEADER { // some older reply coming
							return
						}

						if reply.Success == false {
							// fmt.Println("sendAppendEntries reply failed", rf.me, x)
							if reply.Term > rf.currentTerm {
								// fmt.Println("heartbeat() became follower", rf.me)
								rf.state = STATE_FOLLOWER
								rf.currentTerm = reply.Term
								return
							}

							rf.nextIndex[x]--
							rf.matchIndex[x]--
							rf.heartBeatImmediate = true
							// fmt.Println("reducing match index[", x, "]", rf.matchIndex[x])
							return
						} else {
							rf.matchIndex[x] = args.PrevLogIndex + len(args.Entries)
							rf.nextIndex[x] = rf.matchIndex[x] + 1
							// fmt.Println("success increasing match index[", x, "]", rf.matchIndex[x])
						}

						count := 1
						for j := 0; j < len(rf.peers); j++ {
							if rf.matchIndex[j] >= rf.matchIndex[x] {
								count++
								if count > (len(rf.peers) / 2) {
									// fmt.Println("Start() sendAppendEntries send applyMsg")
									// ** what we the old commit index comes later in time
									rf.commitIndex = rf.matchIndex[x]
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
	// fmt.Println("RequestVote, myid =", rf.me, "cid = ", args.CandidateId)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		// fmt.Println("RequestVote, myid =", rf.me, "cid = ", args.CandidateId, " term less my term ", rf.currentTerm, "candidate term", args.Term)
		return
	}
	if args.Term > rf.currentTerm {
		// become follower and update current term
		// fmt.Println("requestvote() became follower", rf.me)
		rf.state = STATE_FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	// candidate’s log is at least as up-to-date as receiver’s log, grant vote

	idx := len(rf.log) - 1
	term := rf.log[idx].Term
	uptoDate := (args.LastLogTerm > term || (args.LastLogTerm == term && args.LastLogIndex >= idx))
	if ((rf.votedFor == -1) || rf.votedFor == args.CandidateId) && uptoDate {
		// fmt.Println("RequestVote success, myid =", rf.me, "cid = ", args.CandidateId, "error log upto date ", uptoDate,
		// 	"my term", term, "my idx = ", idx, "cad term ", args.LastLogTerm, "cad index", args.LastLogIndex)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.persist()
		return
	}
	// fmt.Println("RequestVote, myid =", rf.me, "cid = ", args.CandidateId, " voted for", rf.votedFor, "error log upto date ", uptoDate)
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
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		// fmt.Println("Append entries fail ", rf.me, "cid = ", args.LeaderId, "less term my term ", rf.currentTerm, "candidate term", args.Term)
		return
	}
	idx := len(rf.log) - 1

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		// fmt.Println("count ", args.DebugCount, "appendentries() became follower", rf.me)
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
	}

	// have doubt about this
	if args.PrevLogIndex > idx {
		// fmt.Println("count ", args.DebugCount, "Append entries fail ", rf.me, "cid = ", args.LeaderId, "log idx incorrect , equal term",
		// 	"prev log index = ", args.PrevLogIndex, " idx = ", idx)
		return
	}

	if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		// fmt.Println("count ", args.DebugCount, "Append entries fail ", rf.me, "cid = ", args.LeaderId, "log idx incorrect , equal term",
		// 	"prev log index = ", args.PrevLogIndex, " idx = ", idx, "term incorrect")
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
		// fmt.Println("count ", args.DebugCount, "append entries ", rf.me, "curridx", currIdx, "commit index", rf.commitIndex)
		if rf.commitIndex >= currIdx {
			return
		}

		rf.log = rf.log[:currIdx]
		rf.log = append(rf.log, args.Entries...)
		// fmt.Println("count ", args.DebugCount, "append entries ", rf.me, "deleted logs", rf.log, args.Entries, args.PrevLogIndex)
		return
	} else {
		rf.log = append(rf.log, args.Entries...)
		// fmt.Println("append entries ", rf.me, "appended logs", rf.log, args.Entries, args.PrevLogIndex)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
		// fmt.Println("count ", args.DebugCount, "AppendEntries ", rf.me, "commit index", rf.commitIndex, rf.log, "entries", args.Entries)
	}

	// if len(args.Entries) > 0 {
	// 	fmt.Println("count ", args.DebugCount, "AppendEntries logs", rf.me, rf.log, "entries", args.Entries, args.PrevLogIndex)
	// }
	rf.persist()
	rf.electionTimeout = rf.getElectionTimeout()
	// fmt.Println("AppendEntries() success ", rf.me)
	return
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
	// fmt.Println("start()", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := rf.state == STATE_LEADER

	if !isLeader {
		// fmt.Println("Start() not leader", rf.me, time.Now().Sub(rf.electionTimeout))
		return index, term, isLeader
	}

	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	// fmt.Println("start() logs", rf.me, command, rf.log)
	index = len(rf.log) - 1
	rf.heartBeatImmediate = true
	return index, term, isLeader
}

func (rf *Raft) applyLog() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			// fmt.Println("apply log()", rf.me, "last applied", rf.lastApplied, "applylog", rf.log)
			m := ApplyMsg{true, rf.log[rf.lastApplied].Command, rf.lastApplied}

			rf.applyCh <- m
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
	rf := &Raft{}
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = STATE_FOLLOWER
	rf.applyCh = applyCh
	rf.debugCount = 0

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.electionTimeout = rf.getElectionTimeout()
	rf.mu.Unlock()
	go rf.startElection()
	go rf.heartBeat()
	go rf.applyLog()

	return rf
}
