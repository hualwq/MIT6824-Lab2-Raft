package myraft

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"labgob/MIT6.824-Lab2-Raft/labgob"
	"labgob/MIT6.824-Lab2-Raft/labrpc"

	"k8s.io/apimachinery/pkg/util/rand"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int // Server当前的term
	voteFor     int // Server在选举阶段的投票目标
	logs        []LogEntry
	nextIndexs  []int // Leader在发送LogEntry时，对应每个其他Server，开始发送的index
	matchIndexs []int
	commitIndex int    // Server已经commit了的Log index
	lastApplied int    // Server已经apply了的log index
	myStatus    Status // Server的状态

	electionTimer  *time.Timer // timer
	heartbeatTimer *time.Timer //timer
	// voteTimeout    time.Duration // 选举超时时间，选举超时时间是会变动的，所以定义在Raft结构体中
	applyChan chan ApplyMsg // 消息channel

	// 2D
	lastIncludeIndex int // snapshot保存的最后log的index
	lastIncludeTerm  int // snapshot保存的最后log的term
	snapshotCmd      []byte
	replicatorCond   []*sync.Cond
	applyCond        *sync.Cond
}

// LogEntry
type LogEntry struct {
	Term  int         // LogEntry中记录有log的Term
	Index int         // LogEntry中log的Index
	Cmd   interface{} // Log的command
}

// 定义一个全局心跳超时时间
var HeartBeatTimeout = 120 * time.Millisecond

type Status int64

const (
	Follower Status = iota
	Candidate
	Leader
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs) // 注意 logs 是 []LogEntry

	return w.Bytes()
}

func (rf *Raft) persist() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	rf.mu.Lock()
	rf.mu.Unlock()
	r := new(bytes.Buffer)
	d := labgob.NewDecoder(r)
	var tmpTerm int
	var tmpVoteFor int
	var tmplogs []LogEntry
	if d.Decode(&tmpTerm) != nil ||
		d.Decode(&tmpVoteFor) != nil ||
		d.Decode(&tmplogs) != nil {
		fmt.Println("decode error")
	} else {
		rf.currentTerm = tmpTerm
		rf.voteFor = tmpVoteFor
		rf.logs = tmplogs
	}
}

func (rf *Raft) getFirstLog() LogEntry {
	return LogEntry{
		Term:  rf.lastIncludeTerm,
		Index: rf.lastIncludeIndex,
	}
}

func (rf *Raft) getLastLog() LogEntry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) matchLog(term int, index int) bool {
	firstIndex := rf.getFirstLog().Index
	// 如果索引小于日志起始索引，说明日志已被截断
	if index < firstIndex {
		return false
	}
	// 如果索引超出当前日志范围
	if index > rf.getLastLog().Index {
		return false
	}
	// 获取对应日志项的任期
	return rf.logs[index-firstIndex].Term == term
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.commitIndex {
		return false
	}

	if lastIncludedIndex > rf.getLastLog().Index {
		rf.logs = make([]LogEntry, 1)
	} else {
		rf.logs = shrinkEntriesArray(rf.logs[lastIncludedIndex-rf.getFirstLog().Index:])
		rf.logs[0].Cmd = nil
	}
	rf.logs[0].Index = lastIncludedIndex
	rf.logs[1].Term = lastIncludedTerm

	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex

	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)

	return true
}

func RandomizedElectionTimeout() time.Duration {
	return time.Duration(300+rand.Intn(150)) * time.Millisecond // 300~450ms
}

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(HeartBeatTimeout) * time.Millisecond
}

func shrinkEntriesArray(entries []LogEntry) []LogEntry {
	newEntries := make([]LogEntry, len(entries))
	copy(newEntries, entries)
	return newEntries
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	snapshotindex := rf.getFirstLog().Index

	if index <= snapshotindex {
		return
	}

	rf.logs = shrinkEntriesArray(rf.logs[index-snapshotindex:])
	rf.logs[0].Cmd = nil
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), rf.snapshotCmd)

}

func (rf *Raft) ChangeState(state Status) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.myStatus = state
}

type VoteErr int64

const (
	Nil                VoteErr = iota //投票过程无错误
	VoteReqOutofDate                  //投票消息过期
	CandidateLogTooOld                //候选人Log不够新
	VotedThisTerm                     //本Term内已经投过票
	RaftKilled                        //Raft程已终止
)

type RequestVoteRequest struct {
	Term         int // 候选人的当前 term
	CandidateId  int // 候选人的 id
	LastLogIndex int // 候选人日志中最后一条日志的 index
	LastLogTerm  int // 候选人日志中最后一条日志的 term
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteResponse struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool    //是否同意投票
	VoteErr     VoteErr //投票操作错误
}

type AppendEntriesErr int64

const (
	AppendErr_Nil          AppendEntriesErr = iota // Append操作无错误
	AppendErr_LogsNotMatch                         // Append操作log不匹配
	AppendErr_ReqOutofDate                         // Append操作请求过期
	AppendErr_ReqRepeat                            // Append请求重复
	AppendErr_Commited                             // Append的log已经commit
	AppendErr_RaftKilled                           // Raft程序终止
)

// snapshot
type InstallSnapshotRequest struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	//Offset         int        // Lab2D不要求实现
	Data []byte
	//Done         	 bool       // Lab2D不要求实现
}

type InstallSnapshotErr int64

const (
	InstallSnapshotErr_Nil InstallSnapshotErr = iota
	InstallSnapshotErr_ReqOutofDate
	InstallSnapshotErr_OldIndex
)

type InstallSnapshotResponse struct {
	Term int
	Err  InstallSnapshotErr
}

type AppendEntriesRequest struct {
	Term         int        // leader 任期
	LeaderId     int        // leader id
	PrevLogIndex int        // leader 中上一次同步的日志索引
	PrevLogTerm  int        // leader 中上一次同步的日志任期
	LeaderCommit int        // 领导者的已知已提交的最高的日志条目的索引
	Entries      []LogEntry // 同步日志
}

type AppendEntriesResponse struct {
	Term          int  // 当前任期号，以便于候选人去更新自己的任期号
	Success       bool // 是否同步成功，true 为成功
	ConflictIndex int  // 冲突index
	ConflictTerm  int  // 冲突Term
}

func (rf *Raft) RequestVote(request *RequestVoteRequest, response *RequestVoteResponse) {

}

func (rf *Raft) advanceCommitIndexForFollower(leaderCommit int) {
	newCommitIndex := min(leaderCommit, rf.getLastLog().Index)
	if newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
	}
}

func (rf *Raft) AppendEntries(request *AppendEntriesRequest, response *AppendEntriesResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if request.Term < rf.currentTerm {
		response.Term, response.Success = rf.currentTerm, false
		return
	}

	if request.Term > rf.currentTerm {
		rf.currentTerm, rf.voteFor = request.Term, -1
	}

	rf.ChangeState(Follower)
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	if request.PrevLogIndex < rf.getFirstLog().Index {
		response.Term, response.Success = 0, false
		return
	}

	if !rf.matchLog(request.PrevLogTerm, request.PrevLogIndex) {
		response.Term, response.Success = rf.currentTerm, false
		lastIndex := rf.getLastLog().Index
		if lastIndex < request.PrevLogIndex {
			response.ConflictTerm, response.ConflictIndex = -1, lastIndex+1
		} else {
			firstIndex := rf.getFirstLog().Index
			response.ConflictTerm = rf.logs[request.PrevLogIndex-firstIndex].Term
			index := request.PrevLogIndex - 1
			for index >= firstIndex && rf.logs[index-firstIndex].Term == response.ConflictIndex {
				index--
			}
			response.ConflictIndex = index
		}
		return
	}

	firstIndex := rf.getFirstLog().Index
	for index, entry := range request.Entries {
		if entry.Index-firstIndex >= len(rf.logs) || rf.logs[entry.Index-firstIndex].Term != entry.Term {
			rf.logs = shrinkEntriesArray(append(rf.logs[:entry.Index-firstIndex], request.Entries[index:]...))
			break
		}
	}
	// 通知上层可以apply主节点已经commit的日志。
	rf.advanceCommitIndexForFollower(request.LeaderCommit)

	response.Term, response.Success = rf.currentTerm, true

}

func (rf *Raft) InstallSnapshot(request *InstallSnapshotRequest, response *InstallSnapshotResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	response.Term = rf.currentTerm

	if request.Term < rf.currentTerm {
		return
	}
	if request.Term > rf.currentTerm {
		rf.currentTerm = request.Term
		rf.voteFor = -1
		rf.persist()
	}
	rf.ChangeState(Follower)
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	if request.LastIncludeIndex <= rf.commitIndex {
		return
	}

	go func() {
		rf.applyChan <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      request.Data,
			SnapshotTerm:  request.LastIncludeTerm,
			SnapshotIndex: request.LastIncludeIndex,
		}
	}()

}

func (rf *Raft) isLogUpToDate(lastLogTerm, lastLogIndex int) bool {
	LastLog := rf.getLastLog()
	myLastLogTerm := LastLog.Index
	myLastLogIndex := LastLog.Term
	return lastLogTerm > myLastLogTerm || (lastLogTerm == myLastLogTerm && lastLogIndex >= myLastLogIndex)
}

func (rf *Raft) sendRequestVote(peer int, request *RequestVoteRequest, response *RequestVoteResponse) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if request.Term < rf.currentTerm || (request.Term == rf.currentTerm && rf.voteFor != -1 && rf.voteFor != request.CandidateId) {
		response.Term, response.VoteGranted = rf.currentTerm, false
		return false
	}
	if request.Term > rf.currentTerm {
		rf.ChangeState(Follower)
		rf.currentTerm, rf.voteFor = request.Term, -1
	}
	// 2A可以先不实现
	if !rf.isLogUpToDate(request.LastLogTerm, request.LastLogIndex) {
		response.Term, response.VoteGranted = rf.currentTerm, false
		return false
	}
	rf.voteFor = request.CandidateId
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	response.Term, response.VoteGranted = rf.currentTerm, true
	return true
}

// func (rf *Raft) sendAppendEntries(server int, request *AppendEntriesRequest, response *AppendEntriesResponse, appendNum *int) bool {
// }

func (rf *Raft) appendNewEntry(command interface{}) *LogEntry {
	newEntry := LogEntry{
		Term:  rf.currentTerm,
		Index: rf.getLastLog().Index + 1,
		Cmd:   command,
	}
	rf.logs = append(rf.logs, newEntry)
	rf.persist()
	return &newEntry
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.myStatus != Leader {
		return -1, -1, false
	}

}

// func (rf *Raft) sendInstallSnapshot(peer int, request *InstallSnapshotRequest, response *InstallSnapshotResponse) bool {

// }

// func (rf *Raft) handleInstallSnapshotResponse(peer int, request *InstallSnapshotRequest, response *InstallSnapshotResponse){

// }

func (rf *Raft) sendInstallSnapshot(peer int, request *InstallSnapshotRequest, response *InstallSnapshotResponse) bool {

}

func (rf *Raft) handleInstallSnapshotResponse(peer int, request *InstallSnapshotRequest, response *InstallSnapshotResponse) {
	if rf.myStatus != Leader {
		return
	}

	if response.Term > rf.currentTerm {
		rf.ChangeState(Follower)
		rf.currentTerm = response.Term
		rf.voteFor = -1
		rf.persist()
		return
	}

	if response.Term == rf.currentTerm {
		// 更新 follower 的 nextIndex 和 matchIndex
		rf.matchIndexs[peer] = Max(rf.matchIndexs[peer], request.LastIncludeIndex)
		rf.nextIndexs[peer] = rf.matchIndexs[peer] + 1
	}
}

func (rf *Raft) sendAppendEntries(peer int, request *AppendEntriesRequest, response *AppendEntriesResponse) bool {

}

func (rf *Raft) advanceCommitIndexForLeader() {
	// 当前日志起点
	firstIndex := rf.getFirstLog().Index
	lastIndex := rf.getLastLog().Index

	// 从后向前遍历候选的 N（更大的优先）
	for N := lastIndex; N > rf.commitIndex; N-- {
		count := 1 // Leader 自己的 matchIndex 始终 ≥ N
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me && rf.matchIndexs[i] >= N {
				count++
			}
		}

		// 如果超过半数服务器的 matchIndex ≥ N，且该日志的 term 是当前 term
		if count > len(rf.peers)/2 && rf.logs[N-firstIndex].Term == rf.currentTerm {
			rf.commitIndex = N
			rf.applyCond.Signal() // 唤醒 applier goroutine
			break
		}
	}
}

func (rf *Raft) handleAppendEntriesResponse(peer int, request *AppendEntriesRequest, response *AppendEntriesResponse) {
	if rf.myStatus == Leader && rf.currentTerm == request.Term {
		if response.Success {
			rf.matchIndexs[peer] = request.PrevLogIndex + len(request.Entries)
			rf.nextIndexs[peer] = rf.matchIndexs[peer] + 1
			rf.advanceCommitIndexForLeader()
		} else {
			if response.Term > rf.currentTerm {
				rf.ChangeState(Follower)
				rf.currentTerm, rf.voteFor = response.Term, -1
				rf.persist()
			} else if response.Term == rf.currentTerm {
				rf.nextIndexs[peer] = response.ConflictIndex
				if response.ConflictTerm != -1 {
					firstIndex := rf.getFirstLog().Index
					for i := request.PrevLogIndex; i >= firstIndex; i-- {
						if rf.logs[i-firstIndex].Term == response.ConflictTerm {
							rf.nextIndexs[peer] = i + 1
							break
						}
					}
				}
			}
		}
	}
}

func (rf *Raft) genRequestVoteRequest() *RequestVoteRequest {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLog := rf.getLastLog()

	return &RequestVoteRequest{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
}

func (rf *Raft) genInstallSnapshotRequest() *InstallSnapshotRequest {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return &InstallSnapshotRequest{
		Term:             rf.currentTerm,
		LeaderId:         rf.me,
		LastIncludeIndex: rf.lastIncludeIndex,
		LastIncludeTerm:  rf.lastIncludeTerm,
		Data:             rf.snapshotCmd,
	}
}

func (rf *Raft) genAppendEntriesRequest(prevLogIndex int) *AppendEntriesRequest {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	firstIndex := rf.getFirstLog().Index

	var prevLogTerm int
	if prevLogIndex >= firstIndex {
		prevLogTerm = rf.logs[prevLogIndex-firstIndex].Term
	} else {
		// prevLogIndex 太旧，说明应该先发 InstallSnapshot，这里设成 -1 也可以崩溃报警
		prevLogTerm = -1
	}

	// 构造要发的 entries：从 nextIndex 开始的日志
	entries := make([]LogEntry, len(rf.logs[prevLogIndex+1-firstIndex:]))
	copy(entries, rf.logs[prevLogIndex+1-firstIndex:])

	return &AppendEntriesRequest{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) StartElection() {
	request := rf.genRequestVoteRequest()
	// use Closure
	grantedVotes := 1
	rf.voteFor = rf.me
	rf.persist()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			response := new(RequestVoteResponse)
			if rf.sendRequestVote(peer, request, response) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm == request.Term && rf.myStatus == Candidate {
					if response.VoteGranted {
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							rf.ChangeState(Leader)
							rf.BroadcastHeartbeat(true)
						}
					} else if response.Term > rf.currentTerm {
						rf.ChangeState(Follower)
						rf.currentTerm, rf.voteFor = response.Term, -1
						rf.persist()
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) BroadcastHeartbeat(force bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.replicateOneRound(peer)
	}
}

func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.Lock()

	if rf.myStatus != Leader {
		rf.mu.Unlock()
		return
	}

	prevLogIndex := rf.nextIndexs[peer] - 1

	if prevLogIndex < rf.getFirstLog().Index {
		request := rf.genInstallSnapshotRequest()
		rf.mu.RUnlock()
		response := new(InstallSnapshotResponse)
		if !rf.sendInstallSnapshot(peer, request, response) {
			rf.mu.Lock()
			rf.handleInstallSnapshotResponse(peer, request, response)
			rf.mu.Unlock()
		}
	} else {
		request := rf.genAppendEntriesRequest(prevLogIndex)
		rf.mu.RUnlock()
		response := new(AppendEntriesResponse)
		if rf.sendAppendEntries(peer, request, response) {
			rf.mu.Lock()
			rf.handleAppendEntriesResponse(peer, request, response)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) Kill() {}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.ChangeState(Candidate)
			rf.currentTerm += 1
			rf.StartElection()
			rf.electionTimer.Reset(RandomizedElectionTimeout())
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.myStatus == Leader {
				rf.BroadcastHeartbeat(true)
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}

	}
}

func Max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		firstLogIndex, commitIndex, LastApplied := rf.getFirstLog().Index, rf.commitIndex, rf.lastApplied
		entries := make([]LogEntry, commitIndex-LastApplied)
		copy(entries, rf.logs[LastApplied-firstLogIndex+1:commitIndex-firstLogIndex+1])
		rf.mu.Unlock()
		for _, entry := range entries {
			rf.applyChan <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Cmd,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.myStatus == Leader && rf.matchIndexs[peer] < rf.getLastLog().Index
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()

	for rf.killed() == false {
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}

		rf.replicateOneRound(peer)
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		myStatus:    Follower,
		voteFor:     -1,
		currentTerm: 0,
		logs:        make([]LogEntry, 1),
		// nextIndexs:       make([]int, 1),
		// matchIndexs:      make([]int, 1),
		electionTimer:    time.NewTimer(RandomizedElectionTimeout()),
		heartbeatTimer:   time.NewTimer(StableHeartbeatTimeout()),
		applyChan:        applyCh,
		dead:             0,
		commitIndex:      0,
		snapshotCmd:      make([]byte, 0),
		lastApplied:      0,
		lastIncludeIndex: -1,
		lastIncludeTerm:  0,
	}

	rf.readPersist(persister.ReadRaftState())
	rf.applyCond = sync.NewCond(&rf.mu)
	LastLog := rf.getLastLog()
	for i := 1; i < len(peers); i++ {
		rf.matchIndexs[i], rf.nextIndexs[i] = 0, LastLog.Index+1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			go rf.replicator(i)
		}
	}
	go rf.ticker()

	go rf.applier()

	return rf
}
