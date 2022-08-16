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
	"math/rand"
	"sync"
	"time"
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
	state           *State        //当前副本的状态
	applyCh         chan ApplyMsg // 返回给tester告诉他该消息以提交
	appendEntriesCh chan []string //发送给leader的AppendEntries, 用来从里面取东西更新ld的log[]
	voteTimeout     int64         //选举超时
	rwMu            sync.RWMutex  //读写锁
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
	CommitIndex int         //当前服务器提交的最大索引
	//LogCommitIndex int         //存在内容的日志的最大索引
	LastApplied int   //当前服务器应用的最大条目
	Identity    int   //身份, 0->leader, 1->follower, 2->candidate
	NextIndex   []int //每个follower下一个log entries的索引, 在当选leader时初始化为leader的最后一个索引+1(即len(Logs))
	MatchIndex  []int //已经同步到follower的日志
}

func (rf *Raft) termAndIdentityCheck(identity, term, nowTerm, index int) bool {
	//选取期间收到心跳, 导致任期变更或者身份变更
	if identity != 2 || nowTerm != term {
		MyPrintf(rf.me, term, index, "[StartVote] identity or term update, shutdown vote, nowTerm=%v, nowIdentity=%v", nowTerm, identity)
		return false
	}
	return true
}

// term 选举发起时的任期
func (rf *Raft) StartVote(term, index int, endTime int64, lastLog *Log) {
	end := make(chan bool)
	box := make(chan bool)
	success := 1
	fail := 0
	nowTime := NowMillSecond()
	//固定选举超时
	go func() {
		time.Sleep(GetMillSecond(endTime - nowTime))
		select {
		case end <- true:
		default:
		}
	}()
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
			ok := false
			for idx := 0; idx < VOTE_REPLACE_CNT; idx++ {
				//MyPrintf(rf.me, term, index, "[StartVote] start call %v", idx)
				//RPC超时就不停重试
				ok = peer.Call("Raft.RequestVote", args, reply)
				//if !ok {
				//	MyPrintf(rf.me, term, index, "[StartVote] call %v rpc timeout", idx)
				//} else {
				//	MyPrintf(rf.me, term, index, "[StartVote] call %v success", idx)
				//}
				if ok {
					break
				}
			}
			//必须用这种写法, 不然携程会阻塞导致积压一堆
			select {
			case box <- reply.Success:
			default:
			}
		}()
	}
	for {
		select {
		case <-end:
			MyPrintf(rf.me, term, index, "[StartVote] vote timeout, shutdown vote")
			return
		case ok := <-box:
			//这部分可有可无, 有了的优点是可以不用等选举超时, 无了的有点是减小串行度(虽然帮助不大吧), 暂时去了吧
			//rf.rwMu.RLock()
			//nowTerm := rf.state.CurrentTerm
			//identity := rf.state.Identity
			//rf.rwMu.RUnlock()
			//if !rf.termAndIdentityCheck(identity,term,nowTerm,index){
			//	return
			//}
			if ok {
				success++
				//收到了大部分投票
				if success*2 > len(rf.peers) {
					rf.rwMu.Lock()
					//真实变更时需要进行二次检测
					nowTerm := rf.state.CurrentTerm
					identity := rf.state.Identity
					// "在一个任期内，如果收到大多数服务器投票，candidate就赢得了选举。"
					//如果任期改变了, 那么这次选举失效
					if !rf.termAndIdentityCheck(identity, term, nowTerm, index) {
						rf.rwMu.Unlock()
						return
					}
					//commitIndex := rf.state.CommitIndex
					nextIndex := len(rf.state.Logs)
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
					rf.state.MatchIndex = matchIndex
					rf.state.NextIndex = nextIndexs
					rf.state.Identity = 0
					MyPrintf(rf.me, term, index, "[StartVote] vote success")
					rf.rwMu.Unlock()
					//todo: 明天重新写下
					go rf.commit()
					for i := 0; i < len(rf.peers); i++ {
						if i == rf.me {
							continue
						}
						go rf.sendHeart(i)
						MyPrintf(rf.me, term, index, "[StartVote] start send log to %v", i)
						go rf.sendLog(i)
					}
					return
				}
			} else {
				fail++
				if fail*2 >= len(rf.peers) {
					MyPrintf(rf.me, term, index, "[StartVote] vote fail")
					return
				}
			}
		}
	}
}

func (rf *Raft) heartCheck() {
	for {
		rf.rwMu.RLock()
		index := len(rf.state.Logs) - 1
		term, ld := rf.GetState()
		died := rf.killed()
		if died {
			MyPrintf(rf.me, term, index, "return [heartCheck]")
			rf.rwMu.RUnlock()
			return
		}
		voteTimeout := rf.voteTimeout
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
				//判断下任期是否发生了改变, 如果改变了那就不开始选举
				nowTerm := rf.state.CurrentTerm
				if nowTerm != term {
					rf.rwMu.Unlock()
					time.Sleep(GetMillSecond(HEART_TIME))
					continue
				}
				rf.voteTimeout = nextVoteTimeout
				rf.state.CurrentTerm++
				rf.state.VotedFor = rf.me
				rf.state.Identity = 2
				lastLog := rf.state.Logs[len(rf.state.Logs)-1]
				//MyPrintf(rf.me, term, index, "[heartCheck], update identity=candidate, update voteTimeout=%v", nextVoteTimeout)
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
	isleader = rf.state.Identity == 0
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
//todo: 明天重新写下
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	defer rf.rwMu.Unlock()
	rf.rwMu.Lock()
	term, _ := rf.GetState()
	index := len(rf.state.Logs) - 1
	log := rf.state.Logs[index]
	died := rf.killed()
	//replyMsg := ""
	//MyPrintf(rf.me, term, index, "get rpc request from %v", args.CandidateId)
	//defer func() {
	//	argBs, _ := json.Marshal(args)
	//	replyBs, _ := json.Marshal(reply)
	//	MyPrintf(rf.me, term, index, "return rpc request,args=%v,reply=%v,replyMsg=%v", string(argBs), string(replyBs), replyMsg)
	//}()
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
		if rf.state.Identity != 1 {
			rf.state.Identity = 1
			MyPrintf(rf.me, term, index, "[RequestVote] get higher term RPC request from %v, update to follower", args.CandidateId)
		}
	}
	//由candidate保证只会对每个follower要一次选票, 这里加rf.state.VotedFor != args.CandidateId的原因是方式回复丢失在网络中
	if rf.state.VotedFor != -1 && rf.state.VotedFor != args.CandidateId {
		//replyMsg = fmt.Sprintf("votefor=%v", rf.state.VotedFor)
		reply.Success = false
		return
	}
	success := false
	//最后一个日志的任期相同, 比较谁的日志更新
	if log.Term == args.LastLogTerm {
		success = log.Index <= args.LastLogIndex
	} else {
		//否则比较谁最后一个日志的任期更新
		success = log.Term < args.LastLogTerm
	}
	if success {
		rf.state.VotedFor = args.CandidateId
		MyPrintf(rf.me, term, index, "[RequestVote] votefor %v", args.CandidateId)
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
	isLeader := true
	var log *Log
	// Your code here (2B).
	rf.rwMu.Lock()
	term, ld := rf.GetState()
	died := rf.killed()
	//其他的一般index都是=rf.state.Logs-1的, 但现在这是个追加log的操作, 追加后index就正确了, emm, 应该咋写都行, 先这样吧
	index = len(rf.state.Logs)
	//leaderCommit := rf.state.CommitIndex
	//lastLog := rf.state.Logs[index - 1]
	if !ld || died {
		isLeader = false
	} else {
		log = &Log{
			Term:    term,
			Index:   index,
			Entries: command,
		}
		rf.state.Logs = append(rf.state.Logs, log)
	}
	rf.rwMu.Unlock()
	if isLeader {
		MyPrintf(rf.me, term, index, "[start] append new log, entries=%v", command)
		//logs := []*Log{log}
		//for i := 0; i < len(rf.peers); i++{
		//	rf.SendAppendEntries(i,term,index,leaderCommit,logs,lastLog,true)
		//}
	}
	return index, term, isLeader
}

func (rf *Raft) sendLog(followerIdx int) {
	for {
		rf.rwMu.RLock()
		term, ld := rf.GetState()
		index := len(rf.state.Logs) - 1
		died := rf.killed()
		if !ld || died {
			MyPrintf(rf.me, term, index, "[sendLog] return isLeader=%v,died=%v", ld, died)
			rf.rwMu.RUnlock()
			return
		}
		leaderCommit := rf.state.CommitIndex
		if len(rf.state.NextIndex) == 0 {
			rf.rwMu.RUnlock()
			MyPrintf(rf.me, term, index, "[sendLog] leader doing init")
			time.Sleep(time.Millisecond * time.Duration(SLEEP_TIME))
			continue
		}
		nextIndex := rf.state.NextIndex[followerIdx]
		matchIndex := rf.state.MatchIndex[followerIdx]
		//理论上不可能
		if nextIndex == 0 {
			panic("err : [sendLog] nextIndex == 0")
		}
		if nextIndex > index {
			rf.rwMu.RUnlock()
			//MyPrintf(rf.me,term,index,"[sendLog] nextIndex >= len(logs)")
			time.Sleep(time.Millisecond * time.Duration(SLEEP_TIME))
			continue
		}
		//暂时先发送一个吧, 以后可以优化
		logs := []*Log{rf.state.Logs[nextIndex]}
		lastLog := rf.state.Logs[nextIndex-1]
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

//收到AppendEntries请求
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	defer func() {
		if reply.ConflictIndex-args.PrevLogIndex > 2 {
			rf.rwMu.Lock()
			argbs, _ := json.Marshal(args)
			replybs, _ := json.Marshal(reply)
			MyPrintf(rf.me, rf.state.CurrentTerm, len(rf.state.Logs), "reply conflict err, args=%v,reply=%v", string(argbs), string(replybs))
			rf.rwMu.Unlock()
		}
	}()
	//自己噶了
	if rf.killed() {
		reply.Err = "died"
		reply.Success = false
		reply.Term = args.Term
		reply.ConflictIndex = args.PrevLogIndex + 1
		return
	}
	defer rf.rwMu.Unlock()
	rf.rwMu.Lock()
	reply.Id = rf.me
	//以收到时为准, 因为rf的CurrentTerm可能会被其他携程更新
	currentTerm, _ := rf.GetState()
	currentIndex := len(rf.state.Logs) - 1
	var preLog *Log
	if currentIndex >= args.PrevLogIndex {
		preLog = rf.state.Logs[args.PrevLogIndex]
	}
	if args.LeaderId == rf.me {
		reply.Term = currentTerm
		reply.Success = true
		reply.ConflictIndex = currentIndex + 1
		return
	}
	reply.Term = currentTerm
	reply.Success = false
	if args.Term < currentTerm {
		reply.ConflictIndex = args.PrevLogIndex + 1
		reply.Err = MyPrintf(rf.me, currentTerm, currentIndex, "[AppendEntries] receiving an overdue request term = %v, prevLogIndex = %v", args.Term, args.PrevLogIndex)
		return
	}
	//更新心跳时间
	voteTimeout := time.Now().UnixNano()/1000000 + rand.Int63n(HEART_TIME*TIMEOUT_CNT) + HEART_TIME*TIMEOUT_CNT
	rf.voteTimeout = voteTimeout
	//MyPrintf(rf.me, currentTerm, currentIndex, "[AppendEntries]get %v heart, update voteTimeout to %v", args.LeaderId, rf.voteTimeout)
	if args.Term > currentTerm {
		//收到更高的RPC心跳, 更新为follower
		nowTerm := rf.state.CurrentTerm
		nowIndex := len(rf.state.Logs) - 1
		//二次判断
		if currentTerm == nowTerm {
			rf.state.CurrentTerm = args.Term
			rf.state.VotedFor = -1
			if identity := rf.state.Identity; identity != 1 {
				rf.state.Identity = 1
				MyPrintf(rf.me, currentTerm, currentIndex, "[AppendEntries] update %v to follower", identity)
			}
			currentTerm = args.Term
		} else {
			//不是发给我的, 让他重试一次
			reply.Err = MyPrintf(rf.me, nowTerm, nowIndex, "[AppendEntries] get past request, target term=%v", currentTerm)
			//todo : 这里的回退lab3可能也会优化
			reply.ConflictIndex = args.PrevLogIndex + 1
			return
		}
	}
	//追随者日志中没有prevLog
	if preLog == nil {
		reply.Err = MyPrintf(rf.me, currentTerm, currentIndex, "[AppendEntries] has not prevLogIndex, prevLogIndex=%v, maxIndex=%v", args.PrevLogIndex, currentIndex)
		reply.ConflictIndex = currentIndex + 1
		return
	}
	//上个日志的任期不匹配
	if preLog.Term != args.PrevLogTerm {
		reply.Err = MyPrintf(rf.me, currentTerm, currentIndex, "[AppendEntries] args.PrevLogTerm.equals(preLog.Term) == false, args.PrevLogTerm=%v, preLog.Term=%v", args.PrevLogTerm, preLog.Term)
		reply.ConflictIndex = args.PrevLogIndex
		return
	}
	//这里校验和追加必须原子化, 不然会小概率出现校验通过后解锁,然后另一个线程追加, 之后本线程追加, 然后造成index和log的下标不一致的情况
	defer rf.rwMu.Unlock()
	rf.rwMu.Lock()
	if len(args.Logs) > 0 {
		nowArgLog := args.Logs[0]
		nowIdx := nowArgLog.Index
		//如果是已存在的条目, 那么判断是第二次发送还是新ld覆盖, 会出现这种情况一定是i==0的时候, 毕竟如果后面的存在了前面的也一定存在
		if nowIdx <= currentIndex {
			nowTerm := rf.state.CurrentTerm
			//不是发给我的
			if nowTerm != currentTerm {
				reply.Err = MyPrintf(rf.me, nowTerm, currentIndex, "[AppendEntries] get past request, target term=%v", currentTerm)
				reply.ConflictIndex = args.PrevLogIndex + 1
				return
			}
			nowLog := rf.state.Logs[nowIdx]
			//二次发送, 让他重试最新的
			if nowArgLog.Term == nowLog.Term {
				reply.Err = MyPrintf(rf.me, nowTerm, currentIndex, "[AppendEntries] This log already exists, log.Index=%v, maxLog.Index=%v", nowArgLog.Index, currentIndex)
				reply.ConflictIndex = currentIndex + 1
				return
			}
			//产生冲突
			nowTerm = rf.state.CurrentTerm
			//是发给我的, 截断现有条目及其之后所以条目
			if nowTerm == currentTerm {
				reply.Err = MyPrintf(rf.me, nowTerm, currentIndex, "[AppendEntries] cut of log, cut of index=%v", nowLog.Index)
				rf.state.Logs = rf.state.Logs[:nowLog.Index]
				reply.ConflictIndex = nowLog.Index
				return
			} else {
				//不是发给我的, 让他重试一次
				reply.Err = MyPrintf(rf.me, nowTerm, currentIndex, "[AppendEntries] get past request, target term=%v", currentTerm)
				//todo : 这里的回退lab3可能也会优化
				reply.ConflictIndex = currentIndex + 1
				return
			}
		}
	}

	//更新commitIndex
	if args.LeaderCommit > rf.state.CommitIndex {
		commitIndex := Min(args.LeaderCommit, len(rf.state.Logs)-1)
		for i := rf.state.CommitIndex + 1; i <= commitIndex; i++ {
			log := rf.state.Logs[i]
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.state.Logs[i].Entries,
				CommandIndex: i,
			}
			//debug完记得去掉
			logs, _ := json.Marshal(args.Logs)
			plog := rf.state.Logs[args.PrevLogIndex]
			prevLog, _ := json.Marshal(plog)
			argBs, _ := json.Marshal(args)
			if log.Index != i || log.Index != applyMsg.CommandIndex {
				applyBs, _ := json.Marshal(applyMsg)
				logBs, _ := json.Marshal(log)
				MyPrintf(rf.me, currentTerm, currentIndex, "[appendentries] index err, applymsg := %v, log := %v, i = %v", string(applyBs), string(logBs), i)
			}
			MyPrintf(rf.me, currentTerm, currentIndex, "[AppendEntries] apply msg %v, command=%v, msgTerm=%v, nowLog=%v,lastIndex=%v,lastTerm=%v,prevLog=%v, args=%v", applyMsg.CommandIndex, applyMsg.Command, rf.state.Logs[i].Term, string(logs), args.PrevLogIndex, args.PrevLogTerm, string(prevLog), string(argBs))
			rf.applyCh <- applyMsg
		}
		rf.state.CommitIndex = commitIndex
		rf.state.LastApplied = Max(rf.state.CommitIndex, rf.state.LastApplied)
		//MyPrintf(rf.me, currentTerm, currentIndex, "[AppendEntries] update commitIndex to %v", rf.state.CommitIndex)
	}
	//追加
	for i := 0; i < len(args.Logs); i++ {
		newLog := args.Logs[i]
		//理论上不可能出现, 捋一下前面俩if就行, 不捋也行, 反正理论不可能出现, 出现了再捋下然后找问题吧
		if newLog.Index != len(rf.state.Logs) {
			MyPrintf(rf.me, currentTerm, currentIndex, "err: [AppendEntries] newLog.Index != len(logs), newLog.Index = %v, len(logs) = %v", newLog.Index, len(rf.state.Logs))
		}
		rf.state.Logs = append(rf.state.Logs, newLog)
	}
	//这里不能等于len(rf.state.Logs), 因为这个request可能在网络中丢失了很久, 引发这种情况
	//request是个心跳, 在网络中待了很久, 因此当前的log增加了很多, 且最后几个是无用log
	//集群正常选举leader同时去除掉无用log, 导致leader的logs的长度小于当前服务器
	//当前服务器收到之前那个请求, prevLog判断无误, 因为是心跳, 不判断args.Log不追加, 然后更改confictIndex为len(logs)这个值比leader的len(logs)多
	//leader错误更新nextIndex, 导致index out of range 的 panic
	confictIndex := args.PrevLogIndex + len(args.Logs) + 1
	reply.Success = true
	reply.ConflictIndex = confictIndex
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.state = &State{
		CurrentTerm: 0,
		VotedFor:    -1,
		Logs:        make([]*Log, 1),
		CommitIndex: 0,
		LastApplied: 0,
		Identity:    1,
	}
	rf.state.Logs[0] = &Log{
		Term:  0,
		Index: 0,
	}
	//ld会不停的读取他, 理论上容量应该是0, 但整个缓存区吧
	rf.appendEntriesCh = make(chan []string, BUFFER_SIZE)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("%v start", rf.me)
	go rf.heartCheck()
	//go rf.ack()
	return rf
}

////定时心跳
//func (rf *Raft) sendHeart() {
//	for {
//		rf.rwMu.Lock()
//		term, ld := rf.GetState()
//		index := len(rf.state.Logs) - 1
//		//这里必须实际改变自己的log, 否则可能会发生任期对不上的情况
//		if !ld {
//			rf.rwMu.Unlock()
//			MyPrintf(rf.me, term, index, "stop send heart")
//			return
//		}
//		log := &Log{
//			Term:  term,
//			Index: index + 1,
//		}
//		rf.state.Logs = append(rf.state.Logs, log)
//		rf.rwMu.Unlock()
//		time.Sleep(GetMillSecond(HEART_TIME))
//	}
//}

//提交日志
//todo: 明天重新写下
func (rf *Raft) commit() {
	for {
		flag := false
		rf.rwMu.RLock()
		term, ld := rf.GetState()
		index := len(rf.state.Logs)
		if !ld || rf.killed() {
			rf.rwMu.RUnlock()
			MyPrintf(rf.me, term, index, "[commit] exit")
			return
		}
		commitIndex := rf.state.CommitIndex
		for i := rf.state.CommitIndex + 1; i < index; i++ {
			//=1是因为一定能得到自己的一票, 这样就不用给自己心跳了
			cnt := 1
			for j := 0; j < len(rf.peers); j++ {
				if rf.state.MatchIndex[j] >= i && j != rf.me {
					cnt++
				}
			}
			if cnt*2 <= len(rf.peers) {
				break
			}
			commitIndex = i
			flag = true
		}
		rf.rwMu.RUnlock()
		//有新条目需要提交
		if flag {
			rf.rwMu.Lock()
			currentTerm, _ := rf.GetState()
			if term == currentTerm {
				MyPrintf(rf.me, term, commitIndex, "[commit] %v commited", commitIndex)
				for i := rf.state.CommitIndex + 1; i <= commitIndex; i++ {
					applyMsg := ApplyMsg{
						CommandValid: true,
						Command:      rf.state.Logs[i].Entries,
						CommandIndex: i,
					}
					rf.applyCh <- applyMsg
					MyPrintf(rf.me, term, commitIndex, "[commit] apply msg %v, command=%v, msgTerm=%v", applyMsg.CommandIndex, applyMsg.Command, rf.state.Logs[i].Term)
				}
				rf.state.CommitIndex = commitIndex
				if rf.state.CommitIndex > rf.state.LastApplied {
					rf.state.LastApplied = rf.state.CommitIndex
				}
			}
			rf.rwMu.Unlock()
		}
		time.Sleep(time.Millisecond * HEART_TIME)
	}
}

//
////很扯淡, 我必须每个副本都通知tester说我已经提交过了, 理论上只用leader响应就可以...但他不信任我leader的响应, 因此引入个这个来不停的提交吧, 每个副本单线程执行
//func (rf *Raft) ack() {
//	for {
//		if rf.killed() {
//			return
//		}
//		rf.rwMu.RLock()
//		term, _ := rf.GetState()
//		index := len(rf.state.Logs)
//		commitIndex := rf.state.CommitIndex
//		commitLogIndex := rf.state.CommitLogIndex
//		for i := commitLogIndex + 1; i <= commitIndex; i++ {
//			//说实话这里不会有什么问题, 毕竟已提交的绝对安全, 但还是上个读锁吧, 不然-race可能过不去
//			log := rf.state.Logs[i]
//			//靠这个判断有点扯淡, 不确定他会不会发个空的...如果有那种情况, 就给日志加个标识来判断是否是心跳吧
//			if log.Entries != nil {
//				rf.state.CommonIndex++
//				applyMsg := ApplyMsg{
//					CommandValid: true,
//					Command:      log.Entries,
//					CommandIndex: rf.state.CommonIndex,
//				}
//				if applyMsg.CommandValid {
//					MyPrintf(rf.me, term, index, "[ack] log commit: term=%v,index=%v,command=%v", log.Term, applyMsg.CommandIndex, applyMsg.Command)
//				}
//				rf.applyCh <- applyMsg
//			}
//		}
//		rf.state.CommitLogIndex = commitIndex
//		rf.rwMu.RUnlock()
//		time.Sleep(GetMillSecond(HEART_TIME))
//	}
//}

func (rf *Raft) sendHeart(followerIdx int) {

	for {
		rf.rwMu.RLock()
		term, ld := rf.GetState()
		index := len(rf.state.Logs) - 1
		if !ld {
			rf.rwMu.RUnlock()
			MyPrintf(rf.me, term, index, "stop send heart")
			return
		}
		nextIndex := rf.state.NextIndex[followerIdx]
		matchIndex := rf.state.MatchIndex[followerIdx]
		leaderCommit := rf.state.CommitIndex
		if nextIndex-1 > index {
			MyPrintf(rf.me, term, index, "index out of range, nextIndex = %v, me.state=%v", nextIndex, rf.state.Identity)
		}
		lastLog := rf.state.Logs[nextIndex-1]
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
			nowTerm, ld := rf.GetState()
			//二次检查
			if !ld || nowTerm != term {
				rf.rwMu.Unlock()
				return 0, nil
			}
			nowNextIndex := rf.state.NextIndex[followerIdx]
			nowMathchIndex := rf.state.MatchIndex[followerIdx]
			if reply.Success {
				//成功简单更新就行
				if nowNextIndex == nextIndex && nowMathchIndex == matchIndex {
					rf.state.NextIndex[followerIdx] = reply.ConflictIndex
					rf.state.MatchIndex[followerIdx] = reply.ConflictIndex - 1
					//可能会打印很多次, 这很正常, 因为有心跳
					//MyPrintf(rf.me, term, index, "[sendAppendEntries] success update %v MatchIndex to %v and update nextIndex to %v", followerIdx, reply.ConflictIndex-1, reply.ConflictIndex)
				}
			} else if reply.Term > term { //任期过期错误
				nowTerm := rf.state.CurrentTerm
				//不是当前任期的RPC直接忽视
				if term == nowTerm {
					//必须优先更新任期
					rf.state.CurrentTerm = reply.Term
					//只要更新任期就重置选票
					rf.state.VotedFor = -1
					//收到更改Term, 退位
					MyPrintf(rf.me, term, index, "[sendAppendEntries] get%v highter term %v, update to follower", followerIdx, reply.Term)
					rf.state.Identity = 1
				}
			} else {
				if nowNextIndex == nextIndex {
					//日志不匹配错误, 回退到期望的日志, todo: 日后可能需要优化, lab3的时候, 如果过不去需要改成term+index的形式
					if reply.ConflictIndex > rf.state.NextIndex[followerIdx] {
						replyBs, _ := json.Marshal(reply)
						MyPrintf(rf.me, term, index, "[SendAppendEntries], confilictIndex err reply=%v", string(replyBs))
					}
					MyPrintf(rf.me, term, index, "[SendAppendEntries] backup %v log %v to %v", followerIdx, rf.state.NextIndex[followerIdx], reply.ConflictIndex)
					if reply.ConflictIndex-1 > nextIndex {
						MyPrintf(rf.me, term, index, "index out of, follower=%v", followerIdx)
					}
					rf.state.NextIndex[followerIdx] = reply.ConflictIndex
				}
			}
			rf.rwMu.Unlock()
			return 0, nil
		}
	}
}
