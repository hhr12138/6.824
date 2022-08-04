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
	"encoding/json"
	"sync"
)
import "sync/atomic"
import "../labrpc"

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
	state   *State        //当前副本的状态
	applyCh chan ApplyMsg // 返回给tester告诉他该消息以提交
}

type Log struct {
	Term    int
	Index   int
	Entries []string
}

type State struct {
	CurrentTerm int         //当前节点认为的最新任期
	VotedFor    interface{} //获得该节点投票的节点标识, 对应ClientEnd的endName
	Logs        []*Log      //存储的日志
	CommitIndex int         //当前服务器提交的最大索引
	LastApplied int         //当前服务器应用的最大条目
}

// return currentTerm and whether this server
// believes it is the leader.
//返回当前服务的状态, 他的任期和他是否认为自己是leader
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
// 请求投票的参数
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
// 请求投票的结果
type RequestVoteReply struct {
	// Your data here (2A).
}

//
// example RequestVote RPC handler.
// 请求别的服务器投票的方法
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
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
// 注意RPC内容的规范, 比如结构体中字段首字母大写, 防止遇上一些低级问题
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
// 每当服务(我们的应用程序)想要追加一个日志就会调用这个Start方法, 如果当前没有leader, 返回false, 否则应当立即返回, 而不是等这个日志被成功提交后才返回
// (wc, 为啥, 那我咋保证返回结果正确....), 因为learder可能不会提交成功(意思是我不需要保证持久化成功就可以返回true?扯淡呢), 确定了多次, 确实是理解返回
// 而不需要等待这个消息被提交, 可能这个的返会并非代表消息提交车工的含义吧
// 如果Raft服务噶了, 需要优雅的返回
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// 第一个返回值是提交后的索引, 第二个是当前任期, 如果当前服务器认为他是leader, 则第三个返回true
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//收到AppendEntries请求
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	//自己噶了
	if rf.killed() {
		reply.Success = false
		reply.Term = args.Term
		reply.ConflictIndex = args.PrevLogIndex + 1
		return nil
	}
	defer rf.mu.Unlock()
	rf.mu.Lock()
	//以收到时为准, 因为rf的CurrentTerm可能会被其他携程更新
	currentTerm := rf.state.CurrentTerm
	reply.Term = currentTerm
	reply.Success = false
	if args.Term < currentTerm {
		reply.ConflictIndex = args.PrevLogIndex + 1
		DPrintf("err: [AppendEntries] 收到过期的请求 term = %v, currentTerm = %v, prevLogIndex = %v", args.Term, currentTerm, args.PrevLogIndex)
		return nil
	}
	if args.Term > currentTerm {
		DPrintf("info: [AppendEntries] 更新currentTerm上锁")
		lastTerm := rf.state.CurrentTerm
		rf.state.CurrentTerm = args.Term
		DPrintf("info: [AppendEntries] 更新currentTerm=%v, 原currentTerm=%v", args.Term, lastTerm)
		rf.state.VotedFor = -1
		DPrintf("info: [AppendEntries] 更新currentTerm解锁")
	}
	if len(rf.state.Logs) <= args.PrevLogIndex {
		DPrintf("err: [AppendEntries] 中不存在PrevLogIndex, PrevLogIndex=%v, 最大索引=%v", args.PrevLogIndex, len(rf.state.Logs))
		reply.ConflictIndex = len(rf.state.Logs)
		return nil
	}
	log := rf.state.Logs[args.PrevLogIndex]
	if log.Term != args.PrevLogTerm {
		DPrintf("err: [AppendEntries] 任期不匹配, prevLogTerm=%v, log.Term=%v", args.PrevLogTerm, log.Term)
		//错误日志的量不会很大, 无需二分查找, 从后往前找几个就行
		for i := len(rf.state.Logs); i >= 0; i-- {
			if rf.state.Logs[i].Term < args.PrevLogTerm {
				reply.ConflictIndex = i + 1
				break
			}
		}
		//删除冲突条目及其之后的所有条目
		rf.state.Logs = rf.state.Logs[:args.PrevLogIndex]
		DPrintf("info: [AppendEntries] 删除了冲突条目及其之后的所有条目, 冲突条目为为%v", reply.ConflictIndex)
		return nil
	} else {
		//日志中存在条目则不追加, Raft仅保证至少一次发送, 因此这是可能的
		argsEntries, _ := json.Marshal(args.Entries)
		rfEntries, _ := json.Marshal(rf.state.Logs[args.PrevLogIndex].Entries)
		DPrintf("warn: [AppendEntries] leader 多次发送相同条目, args.entries=%v, rf.entries=%v", string(argsEntries), string(rfEntries))
		reply.ConflictIndex = len(rf.state.Logs) + 1
		reply.Success = true
	}
	//追加
	newLog := &Log{
		Term:    args.Term,
		Index:   args.PrevLogIndex + 1,
		Entries: args.Entries,
	}
	if newLog.Index != len(rf.state.Logs) {
		DPrintf("err: [AppendEntries] newLog.Index != len(logs), newLog.Index = %v, len(logs) = %v", newLog.Index, len(rf.state.Logs))
	}
	rf.state.Logs = append(rf.state.Logs, newLog)
	reply.Success = true
	reply.ConflictIndex = len(rf.state.Logs) + 1

	//更新commitIndex
	if args.LeaderCommit > rf.state.CommitIndex {
		rf.state.CommitIndex = Min(args.LeaderCommit, len(rf.state.Logs)-1)
		rf.state.LastApplied = Max(rf.state.CommitIndex, rf.state.LastApplied)
	}
	return nil
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
