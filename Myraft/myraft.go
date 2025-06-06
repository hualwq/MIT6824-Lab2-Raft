package myraft

import (

	//	"bytes"
	"sync"
	"time"

	//	"6.824/labgob"
	"labgob/MIT6.824-Lab2-Raft/labrpc"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
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

	timer       *time.Ticker  // timer
	voteTimeout time.Duration // 选举超时时间，选举超时时间是会变动的，所以定义在Raft结构体中
	applyChan   chan ApplyMsg // 消息channel

	// 2D
	lastIncludeIndex int // snapshot保存的最后log的index
	lastIncludeTerm  int // snapshot保存的最后log的term
	snapshotCmd      []byte
}

// LogEntry
type LogEntry struct {
	Term int         // LogEntry中记录有log的Term
	Cmd  interface{} // Log的command
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
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	curterm := rf.currentTerm
	return curterm, rf.myStatus == Leader
}

func (rf *Raft) persist() {

}

func (rf *Raft) readPersist(data []byte) {}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {}

type VoteErr int64

const (
	Nil                VoteErr = iota //投票过程无错误
	VoteReqOutofDate                  //投票消息过期
	CandidateLogTooOld                //候选人Log不够新
	VotedThisTerm                     //本Term内已经投过票
	RaftKilled                        //Raft程已终止
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	Candidate    int
	LastLogIndex int // 用于选举限制，LogEntry中最后Log的index
	LastLogTerm  int // 用于选举限制，LogEntry中最后log的Term
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int //Leader标识
	PrevLogIndex int //nextIndex前一个index
	PrevLogTerm  int //nextindex前一个index处的term
	Logs         []LogEntry
	LeaderCommit int //Leader已经commit了的Log index
	LogIndex     int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool             // Append操作结果
	AppendErr     AppendEntriesErr // Append操作错误情况
	NotMatchIndex int              // 当前Term的第一个元素（没有被commit的元素）的index
}

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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotRequest, reply *InstallSnapshotResponse) {}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteNum *int) bool {
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, appendNum *int) bool {
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotRequest, reply *InstallSnapshotResponse) bool {
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {}

func (rf *Raft) Kill() {}

func (rf *Raft) killed() bool {}

func (rf *Raft) ticker() {}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
}
