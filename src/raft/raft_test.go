package raft

import (
	"../labrpc"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

var rf *Raft
var peers []*labrpc.ClientEnd
var applyCh chan ApplyMsg

func init() {
	applyCh = make(chan ApplyMsg, 100)
	peers := []*labrpc.ClientEnd{
		&labrpc.ClientEnd{},
		&labrpc.ClientEnd{},
		&labrpc.ClientEnd{},
		&labrpc.ClientEnd{},
		&labrpc.ClientEnd{},
	}
	rf = Make(peers, 2, &Persister{}, applyCh)
	go func() {
		for {
			applyMsg, _ := json.Marshal(<-applyCh)
			fmt.Println(string(applyMsg))
		}
	}()
}

//func TestRaft_AppendEntries(t *testing.T) {
//	//初始情况测试
//	args := &AppendEntriesArgs{
//		LeaderId: 1,
//		LeaderCommit: 0,
//		Term: 0,
//		PrevLogIndex: 0,
//		PrevLogTerm: 0,
//	}
//	reply := &AppendEntriesReply{}
//	rf.AppendEntries(args,reply)
//	bsReply,_ := json.Marshal(reply)
//	fmt.Println(string(bsReply))
//	//再次追加5条测试
//	for i := 0; i < 5; i++{
//		args.PrevLogIndex++
//		rf.AppendEntries(args,reply)
//		bsReply,_ = json.Marshal(reply)
//		fmt.Println(string(bsReply))
//	}
//	//二次发送测试
//	args.PrevLogIndex = 3
//	rf.AppendEntries(args,reply)
//	bsReply,_ = json.Marshal(reply)
//	fmt.Println(string(bsReply))
//	//模拟leader更换发送不一样的内容测试(删除测试)
//	args.Term = 1
//	args.PrevLogTerm = 1
//	args.PrevLogIndex = 3
//	rf.AppendEntries(args,reply)
//	bsReply,_ = json.Marshal(reply)
//	fmt.Println(string(bsReply))
//
//	//发送不存在的内容测试
//	args.PrevLogIndex = 100
//	rf.AppendEntries(args,reply)
//	bsReply,_ = json.Marshal(reply)
//	fmt.Println(string(bsReply))
//	//过期leader测试
//	args.Term = 0
//	rf.AppendEntries(args,reply)
//	bsReply,_ = json.Marshal(reply)
//	fmt.Println(string(bsReply))
//	//leader commit更新测试
//	args.LeaderCommit = 1
//	args.Term = 1
//	args.PrevLogIndex = 2 //前面删除了
//	rf.AppendEntries(args,reply)
//	bsReply,_ = json.Marshal(reply)
//	fmt.Println(string(bsReply))
//}

func TestRaft_SendAppendEntries(t *testing.T) {

	for {
		time.Sleep(time.Second * 10)
	}
}
