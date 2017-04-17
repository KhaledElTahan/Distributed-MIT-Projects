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

import "sync"
import "labrpc"
import "time"
import "math/rand"

//import "fmt"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// The log consists of an array of logElement
//
type LogElement struct {
	Command interface{}
	Term    int // The term on which the command was requested
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state (Read all, Write one)
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Persistent State for all servers
	serverState int          // Follower, Candidate or Leader
	currentTerm int          // latest term server has seen "0 initially"
	votedFor    int          // candidateId that received vote in currentTerm (or nil if none)
	log         []LogElement // log entries

	// Volatile State for all servers
	commitIndex       int           // Index of highest log entry known to be commited
	lastApplied       int           // Index of highest log entry applied to state machine
	receivedHeartBeat bool          // If not set then that means I've not received any heartbeats
	leaderId          int           // To be able to redirect clients to Leader
	applyCh           chan ApplyMsg // Channel to which we send commited entries

	// Volatile state on leaders - Initialized after election for leader
	nextIndex  []int // For each server, Index of next log entry to send to that server
	matchIndex []int // For each server, Index of highest log entry known to be replicated on this server
}

const (
	followerState  = 1
	candidateState = 2
	leaderState    = 3
)

//
// Invoked by leader to replicate log enteries; Also used as heartbeats
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term         int          // Leader's term
	LeaderId     int          // So follower can redirect client requests
	PrevLogIndex int          // Index of log entry immediately pereceding new ones
	PrevLogTerm  int          // term of prevLogIndex log entry
	Entries      []LogElement // Log entries to story (Empty for heart beats; May send more than one for efficiency)
	LeaderCommit int          // Leader's Commit Index
}

//
// Reply from the followers to the leader
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term                    int  // currentTerm for leader to update itself
	Success                 bool // true if follower contained entry matching PrevLogIndex & PrevLogTerm
	ConflictTerm            int  // The Conflicting Term for that makes non-agreement
	ConflictTermStartingIdx int  // The starting index of the conflict term interval
	LogLength               int  // If the leader's next index is larger than the follower's log
	Conflict                bool // Does the follower's Log conflict with the Leader's ?
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

//
// Handles when there're entries to append
//
func handleAppendEntries(rf *Raft, args *AppendEntriesArgs, reply *AppendEntriesReply) {

	if args.Entries != nil {
		DPrintf("AppendEntries from (%d) to (%d)\n", args.LeaderId, rf.me)
	} else {
		DPrintf("Heartbeat from (%d) to (%d)\n", args.LeaderId, rf.me)
	}

	if args.PrevLogIndex >= len(rf.log) {
		reply.Conflict = true
		reply.LogLength = len(rf.log)
	} else {
		if args.PrevLogIndex == -1 { //First time ever so we don't need to consider term
			reply.Conflict = false
			rf.log = args.Entries

			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(len(rf.log)-1, args.LeaderCommit)
			}
		} else {
			myTerm := rf.log[args.PrevLogIndex].Term

			if myTerm == args.PrevLogTerm {
				reply.Conflict = false
				rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)

				if args.LeaderCommit > rf.commitIndex {
					rf.commitIndex = min(len(rf.log)-1, args.LeaderCommit)
				}
			} else {
				reply.Conflict = true
				reply.ConflictTerm = myTerm

				for i := args.PrevLogIndex; i >= 0; i-- {
					if rf.log[args.PrevLogIndex].Term == myTerm {
						reply.ConflictTermStartingIdx = i
					} else {
						break
					}
				}

				if reply.ConflictTermStartingIdx < 0 {
					reply.ConflictTermStartingIdx = 0
				}
			}
		}
	}
}

//
// handles an incoming RPC "Receiver"
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	if args.Term < rf.currentTerm { // Leader Failure !
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		reply.Success = true
		reply.Term = args.Term     // So Leader could know if he sent this AppendEntries before failure
		rf.currentTerm = args.Term // Just in case

		rf.receivedHeartBeat = true
		rf.votedFor = -1
		rf.leaderId = args.LeaderId

		handleAppendEntries(rf, args, reply)
	}
	rf.mu.Unlock()

	go applyCommitted(rf)
}

//
// handles an outgoing RPC "Sender"
// The leader is the only sender "Either Fresh or stale"
//
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, entries []LogElement) bool {

	rf.mu.RLock()
	args.Term = rf.currentTerm
	args.LeaderId = rf.me

	reply.ConflictTerm = -1
	reply.ConflictTermStartingIdx = -1
	reply.LogLength = -1

	if entries != nil {
		args.Entries = entries
		args.LeaderCommit = rf.commitIndex

	} else {
		args.PrevLogIndex = len(rf.log) - 1
		args.PrevLogTerm = 0

		if args.PrevLogIndex != -1 {
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		}
	}

	if rf.serverState != leaderState {
		rf.mu.RUnlock()
		return false
	}

	rf.mu.RUnlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.RLock()
	var term = rf.currentTerm
	var isleader = (rf.serverState == leaderState)
	rf.mu.RUnlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int // Candidate's Term
	CandidateId  int // Candidate Requesting Vote
	LastLogIndex int // Index of candidate's last log entry
	LastLogTerm  int // Term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int  // currentTerm for candidate to update himself
	VoteGranted bool // True means candidate received vote
}

//
// Checks if the candidate log is up to date with or more up to date than the Voter's
//
func isUpToDate(candidateLastIndex int, candidateLastTerm int, voterLastIndex int, voterLastTerm int) bool {
	if candidateLastTerm == voterLastTerm {
		if candidateLastIndex >= voterLastIndex {
			return true
		} else {
			return false
		}
	} else {
		if candidateLastTerm >= voterLastTerm {
			return true
		} else {
			return false
		}
	}
}

//
// example RequestVote RPC handler.
// handles an incoming RPC
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()

	if rf.receivedHeartBeat {
		rf.mu.Unlock()
		return
	}

	myLastIndex := len(rf.log) - 1
	myLastTerm := 0
	if myLastIndex != -1 {
		myLastTerm = rf.log[myLastIndex].Term
	}

	if args.Term < rf.currentTerm || rf.votedFor != -1 || !isUpToDate(args.LastLogIndex, args.LastLogTerm, myLastIndex, myLastTerm) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true

	rf.mu.Unlock()
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

	rf.mu.RLock()

	args.CandidateId = rf.me
	args.Term = rf.currentTerm
	args.LastLogIndex = len(rf.log) - 1
	args.LastLogTerm = 0

	if args.LastLogIndex != -1 {
		args.LastLogTerm = rf.log[args.LastLogIndex].Term
	}

	if rf.receivedHeartBeat {
		rf.mu.RUnlock()
		return false
	}

	rf.mu.RUnlock()

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	return ok
}

func printLog(server int, log []LogElement) {
	DPrintf("Log(%d): [", server)

	for i := 0; i < len(log)-1; i++ {
		DPrintf("%d T(%d), ", log[i].Command, log[i].Term)
	}

	if len(log) >= 1 {
		DPrintf("%d T(%d)", log[len(log)-1].Command, log[len(log)-1].Term)
	}

	DPrintf("]\n")
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	if rf.serverState == leaderState {
		DPrintf("START()\n")

		term = rf.currentTerm
		rf.log = append(rf.log, LogElement{command, term})
		index = len(rf.log)

		printLog(rf.me, rf.log)

		rf.mu.Unlock()
	} else {
		rf.mu.Unlock()
		isLeader = false
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	// 3A Section
	rf.serverState = followerState
	rf.currentTerm = 0 //Persistent ?!
	rf.votedFor = -1
	rf.receivedHeartBeat = false

	// 3B Section
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go runSever(rf)

	return rf
}

//
// Timeouts in milli seconds
//
const (
	minimumElectionTimeout  = 300
	maximumElectionTimeout  = 400
	requestVoteReplyTimeout = 200
	heartbeatTimeInterval   = 110
)

func getRandElectionTimeout() time.Duration {
	gap := maximumElectionTimeout - minimumElectionTimeout
	return time.Duration(minimumElectionTimeout + rand.Intn(gap))
}

//
// May functionality the server will run
//
func runSever(rf *Raft) {
	first := true
	for {
		if first {
			time.Sleep(time.Duration(rand.Intn(100)))
			first = false
		} else {
			go applyCommitted(rf)
		}
		rf.mu.RLock()
		if rf.serverState == followerState {
			rf.mu.RUnlock()
			runFollower(rf)
		} else if rf.serverState == candidateState {
			rf.mu.RUnlock()
			runCandidate(rf)
		} else if rf.serverState == leaderState {
			rf.mu.RUnlock()
			runLeader(rf)
		}
	}
}

func applyCommitted(rf *Raft) {
	rf.mu.Lock()
	DPrintf("lastApplied(%d) commitIndex(%d)\n", rf.lastApplied, rf.commitIndex)
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		DPrintf("Me(%d) Apply Command(%d) of Term(%d) & Index(%d)\n", rf.me, rf.log[i].Command, rf.log[i].Term, i)
		msg := ApplyMsg{Command: rf.log[i].Command, Index: i + 1}
		rf.lastApplied++
		rf.applyCh <- msg
	}
	rf.mu.Unlock()
}

func stepDownToFollower(rf *Raft) {
	rf.mu.Lock()
	rf.serverState = followerState
	rf.votedFor = -1
	rf.mu.Unlock()
	time.Sleep(getRandElectionTimeout() * time.Millisecond)
}

func runFollower(rf *Raft) {
	rf.mu.RLock()
	DPrintf("%d is Follower\n", rf.me)
	printLog(rf.me, rf.log)
	if rf.receivedHeartBeat {
		rf.mu.RUnlock()
		rf.mu.Lock()
		rf.receivedHeartBeat = false
		rf.mu.Unlock()
		time.Sleep(getRandElectionTimeout() * time.Millisecond)
	} else {
		rf.mu.RUnlock()
		rf.mu.Lock()
		rf.serverState = candidateState
		rf.mu.Unlock()
		runCandidate(rf)
	}
}

func runCandidate(rf *Raft) {

	rf.mu.Lock()
	DPrintf("%d is Candidate\n", rf.me)
	printLog(rf.me, rf.log)
	if rf.receivedHeartBeat || rf.votedFor != -1 {
		rf.mu.Unlock()
		stepDownToFollower(rf)
	} else {
		rf.currentTerm++
		rf.votedFor = rf.me

		votedToMe := 1
		stepDown := false
		stepDownLock := &sync.RWMutex{}
		for server, _ := range rf.peers {
			if server == rf.me {
				continue
			}

			go func(server int, rf *Raft, stepDownLock *sync.RWMutex) {
				rva := &RequestVoteArgs{}
				rvp := &RequestVoteReply{}
				ok := rf.sendRequestVote(server, rva, rvp)

				if ok {
					rf.mu.Lock()
					if rvp.VoteGranted {
						votedToMe++
					} else if rvp.Term > rf.currentTerm {
						rf.currentTerm = rvp.Term
						stepDownLock.Lock()
						stepDown = true
						stepDownLock.Unlock()
					}
					rf.mu.Unlock()
				}

			}(server, rf, stepDownLock)

			stepDownLock.RLock()
			if stepDown {
				stepDownLock.RUnlock()
				break
			}
			stepDownLock.RUnlock()
		}
		rf.mu.Unlock()

		time.Sleep(time.Duration(requestVoteReplyTimeout) * time.Millisecond)

		stepDownLock.RLock()
		if stepDown {
			stepDownLock.RUnlock()
			stepDownToFollower(rf)
			return
		}
		stepDownLock.RUnlock()

		rf.mu.RLock()
		if rf.receivedHeartBeat {
			rf.mu.RUnlock()
			stepDownToFollower(rf)
			return
		}
		rf.mu.RUnlock()

		rf.mu.Lock()
		if votedToMe > (len(rf.peers) / 2) {
			rf.serverState = leaderState
			DPrintf("%d Became Leader \n", rf.me)
			initializeLeader(rf)
			rf.mu.Unlock()
			runLeader(rf)
			return
		} else { // Lost Election or Split Vote
			rf.currentTerm-- //so next iteration if I become candidate term doesn't go infinite
		}

		rf.votedFor = -1
		rf.mu.Unlock()

		time.Sleep(getRandElectionTimeout() * time.Millisecond)
	}
}

func initializeLeader(rf *Raft) {
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = -1
	}

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
	}
}

func runLeader(rf *Raft) {
	rf.mu.Lock()

	DPrintf("%d is Leader Iteration\n", rf.me)
	printLog(rf.me, rf.log)
	if rf.receivedHeartBeat {
		rf.mu.Unlock()
		stepDownToFollower(rf)
		return
	} else {
		stepDown := false
		stepDownLock := &sync.RWMutex{}

		for server, _ := range rf.peers {
			if server == rf.me {
				continue
			}

			go func(server int, rf *Raft, stepDownLock *sync.RWMutex) {
				aea := &AppendEntriesArgs{}
				aer := &AppendEntriesReply{}

				rf.mu.RLock()
				aea.LeaderCommit = rf.commitIndex
				rf.mu.RUnlock()

				ok := rf.sendAppendEntries(server, aea, aer, nil)
				//			DPrintf("(%d): GO sendAppendEntries to (%d)\n", rf.me, server)

				if ok {
					rf.mu.Lock()
					if !aer.Success && aer.Term != rf.currentTerm { // Only on leader failure
						rf.currentTerm = aer.Term
						stepDownLock.Lock()
						stepDown = true
						rf.serverState = followerState
						rf.votedFor = -1
						stepDownLock.Unlock()
						rf.mu.Unlock()
						return
					} else {
						if aer.Conflict {
							if aer.LogLength != -1 {
								rf.nextIndex[server] = aer.LogLength
							} else {
								change := false

								if aer.ConflictTermStartingIdx < 0 {
									aer.ConflictTermStartingIdx = 0
								}

								for i := len(rf.log) - 1; i >= aer.ConflictTermStartingIdx; i-- {
									if rf.log[i].Term == aer.ConflictTerm { // Match
										rf.nextIndex[server] = i
										change = true
										break
									}
								}

								if !change {
									rf.nextIndex[server] = aer.ConflictTermStartingIdx - 1
									if rf.nextIndex[server] < 0 {
										rf.nextIndex[server] = 0
									}
								}
							}
						} else { // Everything is perfect
							rf.nextIndex[server] = aea.PrevLogIndex + 1
							if aea.PrevLogIndex > rf.matchIndex[server] {
								rf.matchIndex[server] = aea.PrevLogIndex
							}
						}
					}

					rf.mu.Unlock()
				}
			}(server, rf, stepDownLock)
		}

		rf.mu.Unlock()

		stepDownLock.RLock()
		if stepDown {
			stepDownLock.RUnlock()
			stepDownToFollower(rf)
			return
		}
		stepDownLock.RUnlock()

		time.Sleep(time.Duration(heartbeatTimeInterval) * time.Millisecond)
	}

	sendLogEntries(rf)
}

func updateLeaderCommitIndex(rf *Raft) {
	cnt := make(map[int]int)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		if val, ok := cnt[rf.matchIndex[i]]; ok {
			cnt[rf.matchIndex[i]] = val + 1
		} else {
			cnt[rf.matchIndex[i]] = 1
		}
	}

	for k, v := range cnt {
		if v+1 > (len(rf.peers) / 2) {
			if k > rf.commitIndex && rf.log[k].Term == rf.currentTerm {
				rf.commitIndex = k
			}
			break
		}
	}
}

func sendLogEntries(rf *Raft) {
	rf.mu.Lock()

	if rf.serverState != leaderState {
		rf.mu.Unlock()
		return
	}

	updateLeaderCommitIndex(rf)
	stepDown := false
	stepDownLock := &sync.RWMutex{}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		if rf.nextIndex[i] < len(rf.log) { // NextIndex initialy = len(rf.log)
			DPrintf("Server(%d) NextIndex(%d) LenOfLeaderLog(%d)\n", i, rf.nextIndex[i], len(rf.log))
			DPrintf("SEND LOG ENTRIES FROM LEADER (%d) to (%d) Command (%d) Term (%d) \n", rf.me, i, rf.log[rf.nextIndex[i]].Command, rf.log[rf.nextIndex[i]].Term)
			go sendLogEntriesToOneServer(rf, i, &stepDown, stepDownLock)
		}

		stepDownLock.RLock()
		if stepDown {
			stepDownLock.RUnlock()
			stepDownToFollower(rf)
			return
		}
		stepDownLock.RUnlock()
	}

	rf.mu.Unlock()
}

func sendLogEntriesToOneServer(rf *Raft, server int, stepDown *bool, stepDownLock *sync.RWMutex) {

	for {
		aea := &AppendEntriesArgs{}
		aer := &AppendEntriesReply{}
		rf.mu.Lock()

		if rf.serverState != leaderState {
			stepDownLock.Lock()
			*stepDown = true
			stepDownLock.Unlock()
			rf.mu.Unlock()
			break
		}

		if rf.nextIndex[server] == len(rf.log) { // NextIndex initialy = len(rf.log)
			rf.mu.Unlock()
			break
		}

		stepDownLock.RLock()
		DPrintf("%d %d %d %d %d %d\n", rf.me, rf.nextIndex[server], len(rf.log), rf.serverState, rf.receivedHeartBeat, *stepDown)
		stepDownLock.RUnlock()

		entries := rf.log[rf.nextIndex[server]:]

		aea.PrevLogIndex = rf.nextIndex[server] - 1

		if aea.PrevLogIndex != -1 {
			aea.PrevLogTerm = rf.log[aea.PrevLogIndex].Term //might has changed
		} else {
			aea.PrevLogTerm = 0 // Will happen only on first append entry
		}

		futureNextIndex := len(rf.log) - 1
		rf.mu.Unlock()

		ok := rf.sendAppendEntries(server, aea, aer, entries)

		rf.mu.Lock()
		if ok {
			if !aer.Success && aer.Term != rf.currentTerm { // Leader Failure
				rf.currentTerm = aer.Term
				stepDownLock.Lock()
				*stepDown = true
				rf.serverState = followerState
				rf.votedFor = -1
				stepDownLock.Unlock()
				rf.mu.Unlock()
				break
			}

			if aer.Conflict {
				// Two cases to consider
				if aer.LogLength != -1 {
					rf.nextIndex[server] = aer.LogLength

					// RETRY
				} else {
					change := false

					if aer.ConflictTermStartingIdx < 0 {
						aer.ConflictTermStartingIdx = 0
					}

					for i := len(rf.log) - 1; i >= aer.ConflictTermStartingIdx; i-- {
						if rf.log[i].Term == aer.ConflictTerm { // Match
							rf.nextIndex[server] = i
							change = true
							break
						}
					}

					if !change {
						rf.nextIndex[server] = aer.ConflictTermStartingIdx - 1
						if rf.nextIndex[server] < 0 {
							rf.nextIndex[server] = 0
						}
					}

					//RETRY
				}
			} else { // Everything is fine
				rf.nextIndex[server] = futureNextIndex + 1
				rf.matchIndex[server] = futureNextIndex
				rf.mu.Unlock()

				break
			}
		} else {
			rf.mu.Unlock()
			time.Sleep(150 * time.Millisecond)
			rf.mu.Lock()
		}
		rf.mu.Unlock()
	}
}
