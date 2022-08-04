package raft

const (
	HEART_TIME  = 200 //ms
	TIMEOUT_CNT = 5   //超时次数
	BUFFER_SIZE = 100 //缓冲AppendEntries数目
	SLEEP_TIME  = 50  //没新AppendEntries时短暂睡眠会, ms
)

type AppendEntriesArgs struct {
	LeaderId     int  `json:"leader_id"`      //leader的下标
	LeaderCommit int  `json:"leader_commit"`  //以提交的下标
	Term         int  `json:"term"`           //leader的任期
	PrevLogIndex int  `json:"prev_log_index"` //紧跟当前日志的上一个日志的索引
	PrevLogTerm  int  `json:"prev_log_term"`  //紧跟当前日志的上一个日志的任期
	Log          *Log `json:"log"`            //日志条目, 序列化后的值, 如果为null则是心跳
}

type AppendEntriesReply struct {
	Term          int    `json:"term"`           //用于让master更新自己
	ConflictIndex int    `json:"conflict_index"` //用于加速回溯, 让leader更新自己的nextIndex
	Success       bool   `json:"success"`        //是否成功
	Err           string `json:"err"`            //错误的信息
	Id            int    `json:"id"`             //响应者的id
}
