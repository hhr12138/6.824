package raft

const (
	HEART_TIME               = 160 //ms
	TIMEOUT_CNT              = 5   //超时次数
	BUFFER_SIZE              = 100 //缓冲AppendEntries数目
	SLEEP_TIME               = 10  //没新AppendEntries时短暂睡眠会, ms
	VOTE_REPLACE_CNT         = 1   //选举超时最大重试次数
	SEND_LOG_CNT             = 50  //一次RPC发送的最大日志数目
	SEND_SNAPSHOT_SLEEP_TIME = 200
)

type AppendEntriesArgs struct {
	LeaderId     int `json:"leader_id"`     //leader的下标
	LeaderCommit int `json:"leader_commit"` //以提交的下标
	//LeaderLogCommit int  `json:"leader_log_commit"` //存在内容的日志的下标
	Term         int    `json:"term"`           //leader的任期
	PrevLogIndex int    `json:"prev_log_index"` //紧跟当前日志的上一个日志的索引
	PrevLogTerm  int    `json:"prev_log_term"`  //紧跟当前日志的上一个日志的任期
	Logs         []*Log `json:"log"`            //日志条目, 序列化后的值, 如果为null则是心跳, 相当于论文里的entries[]
}

type SnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type SnapshotReply struct {
	Term      int
	NextIndex int
}

type AppendEntriesReply struct {
	Term int `json:"term"` //用于让master更新自己
	//ConflictTerm  int    `json:"conflict_term"`  //用于加速回溯
	ConflictIndex int    `json:"conflict_index"` //用于加速回溯, 让leader更新自己的nextIndex
	Success       bool   `json:"success"`        //是否成功
	Err           string `json:"err"`            //错误的信息
	Id            int    `json:"id"`             //响应者的id
}
