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
	"../labgob"
	"../labrpc"
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"

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
//响应消息, 每当提交了一个消息, 都应当给服务(测试者)(就是你的应用程序或者他用来测试你Raft的测试中)一个ApplyMsg作为响应,
//CommandValid 代表是否有响应消息
//Command 为内容
//CommandIndex 为这个log的下标
//无需响应任期, 包括论文中也说明了只需要响应下标
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogState int

//type LogCache struct{
//	state LogState
//	value interface{}
//}

const (
	NOT_FIND  LogState = 0
	COMMITTED LogState = 1
	WORKING   LogState = 2
)

func init() {
	rand.NewSource(time.Now().UnixNano())
}

//
// A Go object implementing a single Raft peer.
//
//根据下面的me, 把Raft理解为一个单独的Raft副本
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers(副本)
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       *State //当前副本的状态
	CommitIndex int    //当前服务器提交的最大索引
	//LogCommitIndex int         //存在内容的日志的最大索引
	LastApplied int           //当前服务器应用的最大条目
	Identity    int           //身份, 0->leader, 1->follower, 2->candidate
	NextIndex   []int         //每个follower下一个log entries的索引, 在当选leader时初始化为leader的最后一个索引+1(即len(Logs))
	MatchIndex  []int         //已经同步到follower的日志
	applyCh     chan ApplyMsg // 返回给tester告诉他该消息以提交
	//appendEntriesCh chan []string //发送给leader的AppendEntries, 用来从里面取东西更新ld的log[]
	voteTimeout         int64        //选举超时
	rwMu                sync.RWMutex //读写锁
	beforeSnapshotTerm  int          //上次快照的任期, 用来接受日志时的判断
	beforeSnapshotIndex int          //上次快照的下标
}

//快照对象, 任期, 下标, 以及这些快照执行后的内容
type Snapshot struct {
	Term          int
	Index         int
	SnapshotBytes []byte
}

type LogCommand struct {
	RequestId string
	Command
}

type Command struct {
	Ope   string
	Key   string
	Value string
}

type Log struct {
	Term    int
	Index   int
	Entries interface{}
}

type State struct {
	CurrentTerm int         //当前节点认为的最新任期, 需要加锁, 存在并发问题
	VotedFor    interface{} //获得该节点投票的节点标识, 对应ClientEnd的endName
	Logs        []*Log      //存储的日志, 需要加锁, 存在并发问题
}

func (rf *Raft) termAndIdentityCheck(identity, term, nowTerm, index int) bool {
	//选取期间收到心跳, 导致任期变更或者身份变更
	if identity != 2 || nowTerm != term {
		MyPrintf(Info, rf.me, term, index, "[StartVote] identity or term update, shutdown vote, nowTerm=%v, nowIdentity=%v", nowTerm, identity)
		return false
	}
	return true
}

//获取最后一个日志的index
func (rf *Raft) getLastLogIndex() int {
	logSize := len(rf.state.Logs)
	if logSize == 0 {
		return rf.beforeSnapshotIndex
	}
	return rf.state.Logs[logSize-1].Index
}

// term 选举发起时的任期
func (rf *Raft) StartVote(term, index int, endTime int64, lastLog *Log) {
	box := make(chan bool)
	success := 1
	fail := 0
	//固定选举超时
	for i := 0; i < len(rf.peers); i++ {
		//idx := i
		if i == rf.me {
			continue
		}
		peer := rf.peers[i]
		go func() {
			args := &RequestVoteArgs{
				Term:         term,
				LastLogTerm:  lastLog.Term,
				LastLogIndex: lastLog.Index,
				CandidateId:  rf.me,
			}
			reply := &RequestVoteReply{}
			ok := peer.Call("Raft.RequestVote", args, reply)
			if ok {
				box <- reply.Success
			} else {
				MyPrintf(Error, rf.me, term, index, "call %v time out", i)
				box <- false
			}
		}()
	}
	for {
		ok := <-box
		if ok {
			success++
			if success*2 > len(rf.peers) {
				break
			}
		} else {
			fail++
			if fail*2 > len(rf.peers) {
				MyPrintf(Info, rf.me, term, index, "vote fail")
				return
			}
		}
	}
	// "在一个任期内，如果收到大多数服务器投票，candidate就赢得了选举。"
	nextIndex := rf.getLastLogIndex() + 1
	nextIndexs := make([]int, len(rf.peers))
	for i := 0; i < len(nextIndexs); i++ {
		nextIndexs[i] = nextIndex
	}
	matchIndex := make([]int, len(rf.peers))
	for i := 0; i < len(matchIndex); i++ {
		//这里本来是更新成rf.state.CommitIndex的, 但有这种情况
		//5个集群, 死了俩
		//3个不停更新commitIndex->极大的数, 然后重新选主把5个的MatchIndex全部更新了
		//然后死了的俩活了, 之后给了他们错误的leaderCommit, 然后G了
		matchIndex[i] = -1
	}
	rf.rwMu.Lock()
	defer func() {
		MyPrintf(Lock, rf.me, -1, -1, "StartVote out lock")
		rf.rwMu.Unlock()
	}()
	//真实变更时需要进行二次检测
	nowTerm := rf.state.CurrentTerm
	identity := rf.Identity
	//如果任期改变了, 那么这次选举失效
	if !rf.termAndIdentityCheck(identity, term, nowTerm, index) {
		return
	}
	rf.MatchIndex = matchIndex
	rf.NextIndex = nextIndexs
	rf.Identity = 0
	MyPrintf(Info, rf.me, term, index, "[StartVote] vote success")
	//todo: 明天重新写下
	go rf.commit()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendHeart(i)
		MyPrintf(Info, rf.me, term, index, "[StartVote] start send log to %v", i)
		go rf.sendLog(i)
	}
}

func (rf *Raft) heartCheck() {
	for {
		rf.rwMu.RLock()
		//MyPrintf(Lock, rf.me, -1, -1, "heartCheck in Readlock")
		term, ld := rf.GetState()
		index := rf.beforeSnapshotIndex
		if len(rf.state.Logs)-1 >= 0 {
			index = rf.state.Logs[len(rf.state.Logs)-1].Index
		}
		died := rf.killed()
		if died {
			MyPrintf(Info, rf.me, term, index, "return [heartCheck]")
			//MyPrintf(Lock, rf.me, -1, -1, "heartCheck out ReadLock")
			rf.rwMu.RUnlock()
			return
		}
		voteTimeout := rf.voteTimeout
		//MyPrintf(Lock, rf.me, -1, -1, "heartCheck out ReadLock")
		rf.rwMu.RUnlock()
		//ld没必要检测心跳
		if !ld {
			//MyPrintf(rf.me, term, index, "[heartCheck]")
			now := NowMillSecond()
			//MyPrintf(rf.me, term, index, "[heartCheck] now=%v",now)
			if now > voteTimeout {
				//MyPrintf(rf.me, term, index, "[heartCheck] timeout")
				//发起选举
				nextVoteTimeout := NowMillSecond() + HEART_TIME*TIMEOUT_CNT + rand.Int63n(HEART_TIME*TIMEOUT_CNT)
				rf.rwMu.Lock()
				//MyPrintf(Lock, rf.me, -1, -1, "heartCheck in lock")
				//判断下任期是否发生了改变, 如果改变了那就不开始选举
				nowTerm := rf.state.CurrentTerm
				if nowTerm != term {
					//MyPrintf(Lock, rf.me, -1, -1, "heartCheck out lock")
					rf.rwMu.Unlock()
					time.Sleep(GetMillSecond(HEART_TIME))
					continue
				}
				rf.voteTimeout = nextVoteTimeout
				rf.state.CurrentTerm++
				rf.state.VotedFor = rf.me
				rf.persist()
				rf.Identity = 2
				lastLog := &Log{
					Term:  rf.beforeSnapshotTerm,
					Index: rf.beforeSnapshotIndex,
				}
				if len(rf.state.Logs)-1 >= 0 {
					lastLog = rf.state.Logs[len(rf.state.Logs)-1]
				}
				//MyPrintf(rf.me, term, index, "[heartCheck], update identity=candidate, update voteTimeout=%v", nextVoteTimeout)
				//MyPrintf(Lock, rf.me, -1, -1, "heartCheck out lock")
				rf.rwMu.Unlock()
				go rf.StartVote(term+1, index, nextVoteTimeout, lastLog)
			}
		}
		time.Sleep(GetMillSecond(HEART_TIME))
	}
}

// return currentTerm and whether this server
// believes it is the leader.
//返回当前服务的状态, 他的任期和他是否认为自己是leader
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.state.CurrentTerm
	isleader = rf.Identity == 0
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// 持久化调用的方法, 具体需要持久化哪些内容可以看图二
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.state)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
// 将持久化的内容读取到内存中的方法
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	state := &State{}
	if err := d.Decode(state); err != nil {
		MyPrintf(Error, rf.me, 0, 0, "readPersist decode err, err=%v", err.Error())
	} else {
		rf.state = state
	}
}

func (rf *Raft) loadSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	snapshot := &Snapshot{}
	if err := d.Decode(snapshot); err != nil {
		MyPrintf(Error, rf.me, 0, 0, "loadSnapshot decode err, err=%v", err.Error())
	}
	rf.beforeSnapshotIndex = snapshot.Index
	rf.beforeSnapshotTerm = snapshot.Term
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
// 请求投票的参数
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int `json:"term"`
	LastLogIndex int `json:"last_log_index"`
	LastLogTerm  int `json:"last_log_term"`
	CandidateId  int `json:"candidate_id"`
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
// 请求投票的结果
type RequestVoteReply struct {
	// Your data here (2A).
	Success bool `json:"success"` //是否获得选票
}

//
// example RequestVote RPC handler.
// 请求别的服务器投票的方法
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.rwMu.Lock()
	MyPrintf(Lock, rf.me, -1, -1, "RequestVote in lock")
	defer func() {
		MyPrintf(Lock, rf.me, -1, -1, "RequestVote out lock")
		rf.rwMu.Unlock()
	}()
	term, _ := rf.GetState()
	index := rf.beforeSnapshotIndex
	if len(rf.state.Logs)-1 >= 0 {
		index = rf.state.Logs[len(rf.state.Logs)-1].Index
	} else {
		a := 1
		fmt.Println(a)
	}
	idx := rf.binaryFindLogIdxByIndex(index)
	log := &Log{
		Term:  rf.beforeSnapshotTerm,
		Index: rf.beforeSnapshotIndex,
	}
	if idx >= 0 {
		log = rf.state.Logs[idx]
	}
	died := rf.killed()
	//死亡或者任期更高, 回复false
	if died || args.Term < term {
		reply.Success = false
		//replyMsg = fmt.Sprintf("dead=%v,term=%v", died, term)
		return
	}
	//任期较小, 更新任期
	if args.Term > term {
		rf.state.CurrentTerm = args.Term
		rf.state.VotedFor = -1
		rf.persist()
		if rf.Identity != 1 {
			rf.Identity = 1
			MyPrintf(Warn, rf.me, term, index, "[RequestVote] get higher term RPC request from %v, update to follower", args.CandidateId)
		}
	}
	//由candidate保证只会对每个follower要一次选票, 这里加rf.state.VotedFor != args.CandidateId的原因是方式回复丢失在网络中
	if rf.state.VotedFor != -1 && rf.state.VotedFor != args.CandidateId {
		//replyMsg = fmt.Sprintf("votefor=%v", rf.state.VotedFor)
		MyPrintf(Info, rf.me, term, index, "return false, because vote to %v", rf.state.VotedFor)
		reply.Success = false
		return
	}
	success := false
	//最后一个日志的任期相同, 比较谁的日志更新
	if log.Term == args.LastLogTerm {
		success = log.Index <= args.LastLogIndex
		if !success {
			MyPrintf(Info, rf.me, term, index, "return false, because log.Index > args.LastLogIndex, lastLogIndex=%v", args.LastLogIndex)
		}
	} else {
		//否则比较谁最后一个日志的任期更新
		success = log.Term < args.LastLogTerm
		if !success {
			MyPrintf(Info, rf.me, term, index, "return false, because log.Term > args.LastLogTerm, LastLogTerm=%v", args.LastLogTerm)
		}
	}
	if success {
		rf.state.VotedFor = args.CandidateId
		MyPrintf(Info, rf.me, term, index, "[RequestVote] votefor %v", args.CandidateId)
	} else {
		//replyMsg = fmt.Sprintf("log fail, lastTerm=%v,lastIndex=%v", log.Term, log.Index)
	}
	reply.Success = success
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
// 一个向server发送ReequestVote(投票) RPC的例子
// server是目标服务器在rf.peers[](Raft的全部副本)中的下标
// RPC参数在args中
// 回复在*reply中, 应当由被调用者进行填充(即目标server)
// 调用者传递的类型与被调用者应答的类型必须相同(废话, 不相同报错)
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// labrpc包模拟了一个正常的不可靠网络, 请求和应答可能会丢失
// call方法发送请求并等待响应, 如果一起正常返回true, 如果超时则返回false, 因此call可能不会立即返回
// 一个false响应可能由服务器色网, 服务器暂时不可达, 请求/响应在网络中丢失造成
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// 除非被请求的服务器没有返回, 否则Call()保证一定会返回, 无需自己处理超时时间
// look at the comments in ../labrpc/labrpc.go for more details.
// 查看../labrpc/labrpc.go中的注释了解更多细节
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
// 每当服务(我们的应用程序)想要追加一个日志就会调用这个Start方法, 如果当前没有leader, 返回false, 否则应当立即返回, 而不是等这个日志被成功提交后才返回
// (wc, 为啥, 那我咋保证返回结果正确....), 因为learder可能不会提交成功(意思是我不需要保证持久化成功就可以返回true?扯淡呢), 确定了多次, 确实是理解返回
// 而不需要等待这个消息被提交, 可能这个的返会并非代表消息提交成功的含义吧
// 如果Raft服务噶了, 需要优雅的返回
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// 第一个返回值是提交后的索引, 第二个是当前任期, 如果当前服务器认为他是leader, 则第三个返回true
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	var log *Log
	rf.rwMu.Lock()
	MyPrintf(Lock, rf.me, -1, -1, "Start in lock")
	defer func() {
		MyPrintf(Lock, rf.me, -1, -1, "Start out lock")
		rf.rwMu.Unlock()
	}()
	//其他的一般index都是=rf.state.Logs-1的, 但现在这是个追加log的操作, 追加后index就正确了, emm, 应该咋写都行, 先这样吧
	index = rf.beforeSnapshotIndex + 1
	if len(rf.state.Logs)-1 >= 0 {
		index = rf.state.Logs[len(rf.state.Logs)-1].Index + 1
	}
	term, ld := rf.GetState()
	died := rf.killed()
	if !ld || died {
		return -1, -1, false
	}
	//leaderCommit := rf.state.CommitIndex
	//lastLog := rf.state.Logs[index - 1]
	log = &Log{
		Term:    term,
		Index:   index,
		Entries: command,
	}
	rf.state.Logs = append(rf.state.Logs, log)
	rf.persist()
	bs, ok := command.([]byte)
	if ok {
		MyPrintf(Info, rf.me, term, index, "[start] append new log, entries=%v", string(bs))
	}
	return index, term, true
}

func (rf *Raft) sendLog(followerIdx int) {
	for {
		rf.rwMu.RLock()
		//MyPrintf(Lock, rf.me, -1, -1, "sendLog in Readlock")
		term, ld := rf.GetState()
		index := rf.beforeSnapshotIndex
		if len(rf.state.Logs)-1 >= 0 {
			index = rf.state.Logs[len(rf.state.Logs)-1].Index
		}
		died := rf.killed()
		if !ld || died {
			MyPrintf(Info, rf.me, term, index, "[sendLog] return isLeader=%v,died=%v", ld, died)
			//MyPrintf(Lock, rf.me, -1, -1, "sendLog out ReadLock")
			rf.rwMu.RUnlock()
			return
		}
		leaderCommit := rf.CommitIndex
		if len(rf.NextIndex) == 0 {
			//MyPrintf(Lock, rf.me, -1, -1, "sendLog out ReadLock")
			rf.rwMu.RUnlock()
			MyPrintf(Info, rf.me, term, index, "[sendLog] leader doing init")
			time.Sleep(time.Millisecond * time.Duration(SLEEP_TIME))
			continue
		}
		nextIndex := rf.NextIndex[followerIdx]
		matchIndex := rf.MatchIndex[followerIdx]
		//这个已经被快照同步了, 无法发送
		if nextIndex <= rf.beforeSnapshotIndex {
			//读取当前快照
			snapshot := rf.persister.ReadSnapshot()
			arg := &SnapshotArgs{
				Term:              term,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.beforeSnapshotIndex,
				LastIncludedTerm:  rf.beforeSnapshotTerm,
				Data:              snapshot,
			}
			peer := rf.peers[followerIdx]
			//MyPrintf(Lock, rf.me, -1, -1, "sendLog out ReadLock")
			rf.rwMu.RUnlock()
			reply := &SnapshotReply{}
			ok := peer.Call("Raft.SnapshotForLeader", arg, reply)
			if !ok {
				time.Sleep(SEND_SNAPSHOT_SLEEP_TIME * time.Millisecond)
				continue
			}
			MyPrintf(Info, rf.me, term, index, "send snapshot finish")
			rf.rwMu.Lock()
			MyPrintf(Lock, rf.me, -1, -1, "sendLog in lock")
			currentTerm, isLeader := rf.GetState()
			if !isLeader {
				MyPrintf(Lock, rf.me, -1, -1, "sendLog out lock")
				rf.rwMu.Unlock()
				return
			}
			if currentTerm < reply.Term {
				//必须优先更新任期
				rf.state.CurrentTerm = reply.Term
				//只要更新任期就重置选票
				rf.state.VotedFor = -1
				rf.persist()
				//收到更改Term, 退位
				MyPrintf(Info, rf.me, term, index, "[sendLog] get%v highter term %v, update to follower", followerIdx, reply.Term)
				rf.Identity = 1
				MyPrintf(Lock, rf.me, -1, -1, "sendLog out lock")
				rf.rwMu.Unlock()
				return
			}
			rf.NextIndex[followerIdx] = reply.NextIndex
			MyPrintf(Lock, rf.me, -1, -1, "sendLog out lock")
			rf.rwMu.Unlock()
			time.Sleep(SEND_SNAPSHOT_SLEEP_TIME * time.Millisecond)
			continue
		}
		//理论上不可能
		if nextIndex == 0 {
			panic("err : [sendLog] nextIndex == 0")
		}
		if nextIndex > index {
			//MyPrintf(Lock, rf.me, -1, -1, "sendLog out ReadLock")
			rf.rwMu.RUnlock()
			//MyPrintf(rf.me,term,index,"[sendLog] nextIndex >= len(logs)")
			time.Sleep(time.Millisecond * time.Duration(SLEEP_TIME))
			continue
		}
		//暂时先发送一个吧, 以后可以优化
		logs := make([]*Log, 0)
		idx := rf.binaryFindLogIdxByIndex(nextIndex)
		end := rf.binaryFindLogIdxByIndex(index)
		for i := idx; i < nextIndex+SEND_LOG_CNT; i++ {
			if i > end {
				break
			}
			log := rf.state.Logs[i]
			logs = append(logs, log)
		}
		lastLog := &Log{
			Term:  rf.beforeSnapshotTerm,
			Index: rf.beforeSnapshotIndex,
		}
		if idx-1 >= 0 {
			lastLog = rf.state.Logs[idx-1]
		}
		//MyPrintf(Lock, rf.me, -1, -1, "sendLog out ReadLock")
		rf.rwMu.RUnlock()

		//这个打印在过了之后去掉
		//bs,_ := json.Marshal(logs[0].Entries)
		//MyPrintf(rf.me,term,index,"[sendLog] send log %v to %v, len(bs)=%v", logs[0].Index,followerIdx,len(bs))

		sleepTime, _ := rf.SendAppendEntries(followerIdx, term, index, leaderCommit, nextIndex, matchIndex, logs, lastLog)
		if sleepTime != 0 {
			time.Sleep(time.Millisecond * time.Duration(sleepTime))
		}
	}
}

func (rf *Raft) getFirstLogInTheTerm(targetTerm int) int {
	left := 0
	right := len(rf.state.Logs) - 1
	maxLen := right
	for left <= right {
		mid := (left + right) >> 1
		term := rf.state.Logs[mid].Term
		if term < targetTerm {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return Min(right+1, maxLen)
}

//收到AppendEntries请求
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//自己噶了
	if rf.killed() {
		reply.Err = "died"
		reply.Success = false
		reply.Term = args.Term
		reply.ConflictIndex = args.PrevLogIndex + 1
		return
	}
	//defer func() {
	//	//MyPrintf(Lock, rf.me, -1, -1, "appendEntries out lock")
	//	rf.rwMu.Unlock()
	//}()
	rf.rwMu.Lock()
	//MyPrintf(Lock, rf.me, -1, -1, "AppendEntries in lock")
	reply.Id = rf.me
	//以收到时为准, 因为rf的CurrentTerm可能会被其他携程更新
	currentTerm, _ := rf.GetState()
	currentIndex := rf.beforeSnapshotIndex
	if len(rf.state.Logs)-1 >= 0 {
		currentIndex = rf.state.Logs[len(rf.state.Logs)-1].Index
	}
	preLogTerm := rf.beforeSnapshotTerm
	if currentIndex >= args.PrevLogIndex {
		idx := rf.binaryFindLogIdxByIndex(args.PrevLogIndex)
		if idx >= 0 {
			preLogTerm = rf.state.Logs[idx].Term
		}
	}
	if args.LeaderId == rf.me {
		reply.Term = currentTerm
		reply.Success = true
		reply.ConflictIndex = args.PrevLogIndex + len(args.Logs) + 1
		rf.rwMu.Unlock()
		return
	}
	reply.Term = currentTerm
	reply.Success = false
	if args.Term < currentTerm {
		reply.ConflictIndex = args.PrevLogIndex + 1
		reply.Err = MyPrintf(Warn, rf.me, currentTerm, currentIndex, "[AppendEntries] receiving an overdue request term = %v, prevLogIndex = %v", args.Term, args.PrevLogIndex)
		rf.rwMu.Unlock()
		return
	}
	//更新心跳时间
	voteTimeout := time.Now().UnixNano()/1000000 + rand.Int63n(HEART_TIME*TIMEOUT_CNT) + HEART_TIME*TIMEOUT_CNT
	rf.voteTimeout = voteTimeout
	//MyPrintf(rf.me, currentTerm, currentIndex, "[AppendEntries]get %v heart, update voteTimeout to %v", args.LeaderId, rf.voteTimeout)
	if args.Term > currentTerm {
		//收到更高的RPC心跳, 更新为follower
		rf.state.CurrentTerm = args.Term
		rf.state.VotedFor = -1
		rf.persist()
		if identity := rf.Identity; identity != 1 {
			rf.Identity = 1
			MyPrintf(Info, rf.me, currentTerm, currentIndex, "[AppendEntries] update %v to follower", identity)
		}
		currentTerm = args.Term
	}
	//追随者日志中没有prevLog
	if currentIndex < args.PrevLogIndex {
		reply.Err = MyPrintf(Error, rf.me, currentTerm, currentIndex, "[AppendEntries] has not prevLogIndex, prevLogIndex=%v, maxIndex=%v", args.PrevLogIndex, currentIndex)
		reply.ConflictIndex = currentIndex + 1
		rf.rwMu.Unlock()
		return
	}
	//上个日志的任期不匹配
	if preLogTerm != args.PrevLogTerm {
		reply.Err = MyPrintf(Error, rf.me, currentTerm, currentIndex, "[AppendEntries] args.PrevLogTerm.equals(preLog.Term) == false, args.PrevLogTerm=%v, preLog.Term=%v", args.PrevLogTerm, preLogTerm)
		idx := rf.binaryFindLogIdxByIndex(args.PrevLogIndex)
		targetTerm := rf.state.Logs[idx].Term
		conflictIndex := rf.getFirstLogInTheTerm(targetTerm)
		if conflictIndex > args.PrevLogIndex {
			MyPrintf(Error, rf.me, currentTerm, currentIndex, "prevLogIndex=%v, conflictIndex=%v", args.PrevLogIndex, conflictIndex)
		}
		//reply.ConflictIndex = args.PrevLogIndex
		reply.ConflictIndex = conflictIndex
		rf.rwMu.Unlock()
		return
	}
	//追加
	for i := 0; i < len(args.Logs); i++ {
		newLog := args.Logs[i]
		//不用currentIndex比较了, 还得实时维护, 多len下没啥问题
		nowIdx := rf.beforeSnapshotIndex + 1
		if len(rf.state.Logs)-1 >= 0 {
			nowIdx = rf.state.Logs[len(rf.state.Logs)-1].Index + 1
		}
		if newLog.Index < nowIdx {
			nowLog := rf.state.Logs[rf.binaryFindLogIdxByIndex(newLog.Index)]
			if newLog.Term != nowLog.Term {
				nowIndex := Max(rf.binaryFindLogIdxByIndex(nowLog.Index), 0)
				rf.state.Logs = rf.state.Logs[:nowIndex]
			} else {
				//没截断就继续吧
				continue
			}
		}
		rf.state.Logs = append(rf.state.Logs, newLog)
	}
	rf.persist()
	//更新commitIndex, 并提交
	msgs := make([]*ApplyMsg, 0)
	if args.LeaderCommit > rf.CommitIndex {
		nowIdx := rf.beforeSnapshotIndex
		if len(rf.state.Logs)-1 >= 0 {
			nowIdx = rf.state.Logs[len(rf.state.Logs)-1].Index
		}
		commitIndex := Min(args.LeaderCommit, nowIdx)
		start := rf.binaryFindLogIdxByIndex(rf.CommitIndex) + 1
		end := rf.binaryFindLogIdxByIndex(commitIndex)
		for i := start; i <= end; i++ {
			log := rf.state.Logs[i]
			applyMsg := &ApplyMsg{
				CommandValid: true,
				Command:      rf.state.Logs[i].Entries,
				CommandIndex: log.Index,
			}
			//if log.Index != i || log.Index != applyMsg.CommandIndex {
			//	applyBs, _ := json.Marshal(applyMsg)
			//	logBs, _ := json.Marshal(log)
			//	MyPrintf(Error, rf.me, currentTerm, currentIndex, "[appendentries] index err, applymsg := %v, log := %v, i = %v", string(applyBs), string(logBs), i)
			//}
			msgs = append(msgs, applyMsg)
		}
		//MyPrintf(rf.me, currentTerm, currentIndex, "[AppendEntries] update commitIndex to %v", rf.state.CommitIndex)
	}
	rf.rwMu.Unlock()
	for i := 0; i < len(msgs); i++ {
		applyMsg := msgs[i]
		rf.applyCh <- *applyMsg
		MyPrintf(Info, rf.me, currentTerm, currentIndex, "[commit] apply msg %v, command=%v", applyMsg.CommandIndex, applyMsg.Command)
		rf.rwMu.Lock()
		rf.CommitIndex = applyMsg.CommandIndex
		rf.LastApplied = Max(rf.CommitIndex, rf.LastApplied)
		rf.rwMu.Unlock()
	}
	confictIndex := args.PrevLogIndex + len(args.Logs) + 1
	reply.Success = true
	reply.ConflictIndex = confictIndex
	//rf.rwMu.Unlock()
	return
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
// 为了方便, 测试的case不会在测试后真正的杀死携程, 而是调用kill方法来模拟, 我可以通过killed()方法来判断他是否被杀死
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
// 每个携程在长时间运行时应该经常调用killed()(比如每次循环), 来判断自己是否被杀死, 从而避免过多占用CPU或者产生另人困惑的输出
func (rf *Raft) Kill() {
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
// 通过Make来创建一个新的Raft副本, 保证所有服务器上的peers的顺序一致, Raft服务需要往applyCh上发送ApplyMsg(详见ApplyMsg), Make()方法必须
// 尽快返回, 因此需要把长时间的任务交给别的携程并行执行
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		CommitIndex: 0,
		LastApplied: 0,
		Identity:    1,
		peers:       peers,
		persister:   persister,
		me:          me,
		applyCh:     applyCh,
	}

	// Your initialization code here (2A, 2B, 2C).
	rf.state = &State{
		CurrentTerm: 0,
		VotedFor:    -1,
		Logs:        make([]*Log, 1),
	}
	rf.state.Logs[0] = &Log{
		Term:  0,
		Index: 0,
	}
	////ld会不停的读取他, 理论上容量应该是0, 但整个缓存区吧
	//rf.appendEntriesCh = make(chan []string, BUFFER_SIZE)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.loadSnapshot(persister.ReadSnapshot())

	DPrintf("%v start", rf.me)
	go rf.heartCheck()
	//go rf.ack()
	return rf
}

//根据日志的index来查找他在当前logs中的任期
func (rf *Raft) binaryFindLogIdxByIndex(index int) int {
	//MyPrintf(Critcal,rf.me,rf.state.CurrentTerm,-1,"start binary find %v",index)
	left := 0
	right := len(rf.state.Logs) - 1
	for left <= right {
		mid := (left + right) >> 1
		if rf.state.Logs[mid].Index <= index {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	//MyPrintf(Critcal,rf.me,rf.state.CurrentTerm,-1,"end binary find %v",index)
	return left - 1
}

//提交日志
//todo: 明天重新写下
func (rf *Raft) commit() {
	for {
		flag := false
		rf.rwMu.RLock()
		//MyPrintf(Lock, rf.me, -1, -1, "commit in Readlock")
		term, ld := rf.GetState()
		index := rf.beforeSnapshotIndex + 1
		if len(rf.state.Logs)-1 >= 0 {
			index = rf.state.Logs[len(rf.state.Logs)-1].Index + 1
		}
		if !ld || rf.killed() {
			//MyPrintf(Lock, rf.me, -1, -1, "commit out ReadLock")
			rf.rwMu.RUnlock()
			MyPrintf(Info, rf.me, term, index, "[commit] exit")
			return
		}
		start := rf.binaryFindLogIdxByIndex(rf.CommitIndex) + 1
		end := rf.binaryFindLogIdxByIndex(index) + 1
		commitLogIndex := start - 1
		for i := start; i < end; i++ {
			cnt := 1
			log := rf.state.Logs[i]
			for j := 0; j < len(rf.peers); j++ {
				if rf.MatchIndex[j] >= log.Index && j != rf.me {
					cnt++
				}
			}
			if cnt*2 <= len(rf.peers) {
				break
			}
			commitLogIndex = i
			flag = true
		}
		//MyPrintf(Lock, rf.me, -1, -1, "commit out ReadLock")
		//有新条目需要提交, 每个leader只提交自己任期的日志(见Figure 8)
		commitLog := &Log{}
		if flag {
			commitLog = rf.state.Logs[commitLogIndex]
		}
		msgs := make([]*ApplyMsg, 0, commitLogIndex-start+1)
		if flag && commitLog.Term == term {
			MyPrintf(Lock, rf.me, -1, -1, "commit in lock")
			for i := start; i <= commitLogIndex; i++ {
				applyMsg := &ApplyMsg{
					CommandValid: true,
					Command:      rf.state.Logs[i].Entries,
					CommandIndex: rf.state.Logs[i].Index,
				}
				msgs = append(msgs, applyMsg)
			}
		}
		rf.rwMu.RUnlock()
		if len(msgs) != 0 {
			for i := 0; i < len(msgs); i++ {
				applyMsg := *msgs[i]
				rf.applyCh <- applyMsg
				MyPrintf(Critcal, rf.me, term, commitLog.Index, "[commit] apply msg %v, command=%v", applyMsg.CommandIndex, applyMsg.Command)
				rf.rwMu.Lock()
				nowTerm, _ := rf.GetState()
				//只有这一个线程会commit, 因此无需进行额外判断
				rf.CommitIndex = commitLog.Index
				if rf.CommitIndex > rf.LastApplied {
					rf.LastApplied = rf.CommitIndex
				}
				if nowTerm != term {
					MyPrintf(Warn, rf.me, nowTerm, index, "commit() term change")
					rf.rwMu.Unlock()
					return
				}
				rf.rwMu.Unlock()
			}
		}
		//if flag{
		//	rf.rwMu.Lock()
		//	nowTerm,_ := rf.GetState()
		//	if nowTerm != term{
		//		MyPrintf(Warn,rf.me,nowTerm,index,"commit() term change")
		//		rf.rwMu.Unlock()
		//		return
		//	}
		//	//只有这一个线程会commit, 因此无需进行额外判断
		//	rf.CommitIndex = commitLog.Index
		//	if rf.CommitIndex > rf.LastApplied {
		//		rf.LastApplied = rf.CommitIndex
		//	}
		//	rf.rwMu.Unlock()
		//}
		MyPrintf(Lock, rf.me, -1, -1, "commit out lock")
		time.Sleep(time.Millisecond * HEART_TIME)
	}
}

func (rf *Raft) sendHeart(followerIdx int) {

	for {
		rf.rwMu.RLock()
		//MyPrintf(Lock, rf.me, -1, -1, "sendHeart in Readlock")
		term, ld := rf.GetState()
		index := rf.beforeSnapshotIndex
		if len(rf.state.Logs)-1 >= 0 {
			index = rf.state.Logs[len(rf.state.Logs)-1].Index
		}
		if !ld {
			//MyPrintf(Lock, rf.me, -1, -1, "sendHeart out ReadLock")
			rf.rwMu.RUnlock()
			MyPrintf(Info, rf.me, term, index, "stop send heart")
			return
		}
		nextIndex := rf.NextIndex[followerIdx]
		matchIndex := rf.MatchIndex[followerIdx]
		leaderCommit := rf.CommitIndex
		if nextIndex-1 > index {
			MyPrintf(Error, rf.me, term, index, "index out of range, nextIndex = %v, me.state=%v", nextIndex, rf.Identity)
		}
		idx := rf.binaryFindLogIdxByIndex(nextIndex - 1)
		lastLog := &Log{
			Term:  rf.beforeSnapshotTerm,
			Index: rf.beforeSnapshotIndex,
		}
		if idx >= 0 {
			lastLog = rf.state.Logs[idx]
		}
		//MyPrintf(Lock, rf.me, -1, -1, "sendHeart out ReadLock")
		rf.rwMu.RUnlock()
		rf.SendAppendEntries(followerIdx, term, index, leaderCommit, nextIndex, matchIndex, nil, lastLog)
		time.Sleep(GetMillSecond(HEART_TIME))
	}
}

//term : 发送时任期
func (rf *Raft) SendAppendEntries(followerIdx, term, index, leaderCommit, nextIndex, matchIndex int, logs []*Log, lastLog *Log) (int64, error) {
	//理论上没必要RPC自己, 但特殊处理的代码有点多...一视同仁吧
	peer := rf.peers[followerIdx]
	//lab2的struct里面说为每个peer分配一个携程进行RPC通信, 并且无需自行处理超时, 他会帮我处理
	//但实际情况是他的call最差可能6s才能返回......无法满足论文中说的rpctimeout << election time, 因此自行处理超时
	end := make(chan bool)
	ok := make(chan *AppendEntriesReply)
	//超时时间为election time(选举超时)的1/10以下
	go func() {
		sleepTime := GetMillSecond(HEART_TIME*TIMEOUT_CNT) / 10
		time.Sleep(sleepTime)
		select {
		case end <- true:
		default:
		}
	}()
	//这里不知道这么搞合不合适, 修复了一个问题
	//server1在term4index51提交后和其他服务器断联了(但他和客户端的联系还没问题), 之后重新选举的新master的len(logs)=51
	//master对server1的nextIndex就一直停在52, 然后server1网络波动解决了了, 此时index=101在其余4个server都成功提交了, leaderCommit=101
	//且在此期间server1不停同步客户端的日志到本地, 导致自身的len(logs)=101(反正大于51就行)
	//之后对server1发送心跳, 其中nextIndex=52, prevLogIndex=51, prevLogTerm=4,leadercommit=101
	//然后prevLog的检查通过了, 又因为心跳没有日志条目, 导致日志冲突检查通过了, 不存在新条目所以没追加, 然后就开始更新commit了
	//commit=min(leaderCommit, len(logs))
	//commit被更新成了一个大于51的值, 但实际上server1的commit只有51, 51之后的是错误的日志
	// 说实话, 这和论文中说到的不一致, 但按论文中的没法解决这个问题
	//todo: 先这么实现, 看完老头的课之后再试下, 然后找ys问问
	leaderCommit = Min(leaderCommit, matchIndex)
	go func() {
		args := &AppendEntriesArgs{
			LeaderId:     rf.me,
			LeaderCommit: leaderCommit,
			Term:         term,
			PrevLogIndex: lastLog.Index,
			PrevLogTerm:  lastLog.Term,
			//暂时只发一个吧
			Logs: logs,
		}
		reply := &AppendEntriesReply{}
		success := peer.Call("Raft.AppendEntries", args, reply)
		if !success {
			reply = nil
		}
		select {
		case ok <- reply:
		default:
		}
	}()
	select {
	case <-end:
		//MyPrintf(rf.me, term, index, "[SendAppendEntries] rpc to %v timeout", followerIdx)
		return SLEEP_TIME, nil
	case reply := <-ok:
		if reply == nil {
			//MyPrintf(rf.me, term, index, "[SendAppendEntries] rpc timeout")
			return SLEEP_TIME, nil
		} else {
			rf.rwMu.Lock()
			//MyPrintf(Lock, rf.me, -1, -1, "sendAppendEntries in lock")
			nowTerm, ld := rf.GetState()
			//二次检查
			if !ld || nowTerm != term {
				//MyPrintf(Lock, rf.me, -1, -1, "sendAppendEntries out lock")
				rf.rwMu.Unlock()
				return 0, nil
			}
			nowNextIndex := rf.NextIndex[followerIdx]
			nowMathchIndex := rf.MatchIndex[followerIdx]
			if reply.Success {
				//成功简单更新就行
				if nowNextIndex == nextIndex && nowMathchIndex == matchIndex {
					rf.NextIndex[followerIdx] = reply.ConflictIndex
					rf.MatchIndex[followerIdx] = reply.ConflictIndex - 1
					//可能会打印很多次, 这很正常, 因为有心跳
					MyPrintf(Debug, rf.me, term, index, "[sendAppendEntries] success update %v MatchIndex to %v and update nextIndex to %v", followerIdx, reply.ConflictIndex-1, reply.ConflictIndex)
				}
			} else if reply.Term > term { //任期过期错误
				//必须优先更新任期
				rf.state.CurrentTerm = reply.Term
				//只要更新任期就重置选票
				rf.state.VotedFor = -1
				rf.persist()
				//收到更改Term, 退位
				MyPrintf(Info, rf.me, term, index, "[sendAppendEntries] get%v highter term %v, update to follower", followerIdx, reply.Term)
				rf.Identity = 1
			} else {
				if nowNextIndex == nextIndex {
					//日志不匹配错误, 回退到期望的日志, todo: 日后可能需要优化, lab3的时候, 如果过不去需要改成term+index的形式 done
					if reply.ConflictIndex > rf.NextIndex[followerIdx] {
						replyBs, _ := json.Marshal(reply)
						MyPrintf(Error, rf.me, term, index, "[SendAppendEntries], confilictIndex err reply=%v", string(replyBs))
					}
					MyPrintf(Error, rf.me, term, index, "[SendAppendEntries] backup %v log %v to %v", followerIdx, rf.NextIndex[followerIdx], reply.ConflictIndex)
					if reply.ConflictIndex-1 > nextIndex {
						MyPrintf(Error, rf.me, term, index, "index out of, follower=%v", followerIdx)
					}
					//可能会出现已存在然后让nextIndex后移的情况, 但他返回的不一定正确
					//如1当leader把index更新到1000, 然后2,3同步到30
					//1挂了,2当选, 给1发index=30的appendEntries, 1返回个1000
					//2更新nextIndex=1000, 下次发送日志时就挂了
					idx := rf.beforeSnapshotIndex
					if len(rf.state.Logs)-1 >= 0 {
						idx = rf.state.Logs[len(rf.state.Logs)-1].Index
					}
					rf.NextIndex[followerIdx] = Min(reply.ConflictIndex, idx)
				}
			}
			//MyPrintf(Lock, rf.me, -1, -1, "sendAppendEntries out lock")
			rf.rwMu.Unlock()
			return 0, nil
		}
	}
}

//leader传递快照
func (rf *Raft) SnapshotForLeader(args *SnapshotArgs, reply *SnapshotReply) {
	rf.rwMu.Lock()
	MyPrintf(Lock, rf.me, -1, -1, "SnapshotForLeader in lock")
	defer func() {
		MyPrintf(Lock, rf.me, -1, -1, "SnapshotForLeader out lock")
		rf.rwMu.Unlock()
	}()
	currentTerm := rf.state.CurrentTerm
	term := args.Term
	//当前任期更新, 回复这次请求
	reply.Term = currentTerm
	if currentTerm > term {
		return
	}
	logs := make([]*Log, 0)
	//如果现存日志条目和快照中最后的日志条目具有相同的索引值和任期号, 那么就保留其后的日志, 否则就丢弃全部日志
	if len(rf.state.Logs) > 0 && rf.state.Logs[0].Index <= args.LastIncludedIndex && rf.state.Logs[len(rf.state.Logs)-1].Index > args.LastIncludedIndex {
		for i := 0; i < len(rf.state.Logs); i++ {
			log := rf.state.Logs[i]
			if log.Index > args.LastIncludedIndex {
				logs = append(logs, log)
			}
		}
	}

	rf.state.Logs = logs
	rf.beforeSnapshotIndex = args.LastIncludedIndex
	rf.beforeSnapshotTerm = args.LastIncludedTerm
	stateBytes := rf.getStateBytes()
	reply.NextIndex = args.LastIncludedIndex + 1
	if len(rf.state.Logs) > 0 {
		reply.NextIndex = rf.state.Logs[len(rf.state.Logs)-1].Index + 1
	}
	rf.persister.SaveStateAndSnapshot(stateBytes, args.Data)
}

//本地快照
func (rf *Raft) Snapshot(targetIdx int, bs []byte) {
	rf.rwMu.Lock()
	MyPrintf(Lock, rf.me, -1, -1, "Snapshot in lock")
	defer func() {
		rf.rwMu.Unlock()
		MyPrintf(Lock, rf.me, -1, -1, "Snapshot out lock")
	}()
	length := len(rf.state.Logs)
	term := rf.state.CurrentTerm
	index := rf.state.Logs[length-1].Index
	//beforeSnapshotBytes := rf.persister.ReadSnapshot()
	//beforeSnapshot := &Snapshot{}
	//err := json.Unmarshal(beforeSnapshotBytes, beforeSnapshot)
	//if err != nil{
	//	MyPrintf(Critcal,rf.me,term,index,"unmarshal error: %v",err.Error())
	//}
	beforeSnapshotIndex := rf.beforeSnapshotIndex
	//当前快照更加新, 这种情况可能发生在leader的同步和自身同步并发出现的情况下, 此时什么也不做
	if targetIdx <= beforeSnapshotIndex {
		MyPrintf(Warn, rf.me, term, index, "targetIndex < snapshotIndex, targetIndex=%v, snapshotIndex=%v", targetIdx, index)
		return
	}
	//不能直接用targetIdx来当作下标, 因为此时切片的idx和日志的不再一一对于了
	targetTerm := 0
	newLogs := make([]*Log, 0)
	//下面这个for可以优化成二分查找
	for i := 0; i < length; i++ {
		log := rf.state.Logs[i]
		if log.Index == targetIdx {
			targetTerm = log.Term
		}
		//不需要快照的再塞回去
		if log.Index > targetIdx {
			newLogs = append(newLogs, log)
		}
		//logs里的第一个log的下标小于当前下标, 这种情况不可能发生, 测试完后去掉
		if log.Index != 0 && log.Index <= beforeSnapshotIndex {
			MyPrintf(Error, rf.me, term, index, "%v index log is snapshot", i)
			continue
		}
	}
	snapshot := Snapshot{
		Term:          targetTerm,
		Index:         targetIdx,
		SnapshotBytes: bs,
	}
	rf.state.Logs = newLogs
	rf.beforeSnapshotTerm = targetTerm
	rf.beforeSnapshotIndex = targetIdx
	stateBytes := rf.getStateBytes()
	snapshotBytes := rf.getSnapshotBytes(snapshot)
	rf.persister.SaveStateAndSnapshot(stateBytes, snapshotBytes)
}

func (rf *Raft) getStateBytes() []byte {
	//这里死了没问题, 此时的state的状态是临时的, 下次恢复的时候会按上次持久化的state恢复
	stateW := new(bytes.Buffer)
	stateEncoder := labgob.NewEncoder(stateW)
	stateEncoder.Encode(rf.state)
	stateBytes := stateW.Bytes()
	return stateBytes
}

func (rf *Raft) getSnapshotBytes(snapshot Snapshot) []byte {
	snapshotW := new(bytes.Buffer)
	snapshotEncoder := labgob.NewEncoder(snapshotW)
	snapshotEncoder.Encode(snapshot)
	snapshotBytes := snapshotW.Bytes()
	return snapshotBytes
}
