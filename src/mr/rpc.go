package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	IpPort string `json:"ip_port"` //servant的唯一标识
	Free   bool   `json:"free"`    //是否空闲
}

type ExampleReply struct {
	HasTask bool  `json:"has_task"` //是否有任务
	Task    *Task `json:"task"`
}

type MapTaskAck struct {
	Id          int      `json:"id"`
	MapId       int      `json:"map_id"`
	ReduceFiles []string `json:"reduce_files"` //reduce_id到filename的映射, 用来请求master
}

type ReduceTaskAck struct {
	Id         int        `json:"id"`
	ReduceId   int        `json:"reduce_id"`
	WordCounts []KeyValue `json:"word_counts"` //key为单词, value为次数
}

type AckReply struct {
	Success bool `json:"success"`
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
