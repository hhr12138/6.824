package kvraft

import (
	"../labrpc"
	"crypto/rand"
	"fmt"
	"math/big"
	"strconv"
	"sync/atomic"
	"time"
)



type Clerk struct {
	servers []*labrpc.ClientEnd
	leader int64 //当前client认为的leader
	me string
	requestCnt *int64
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	var num int64 = 0
	ck.requestCnt = &num
	//ns级时间戳+10000以内的random来降低概率吧, 正常来说应该调用时指定的
	nanosecond := time.Now().Nanosecond()
	randNum := nrand()
	ck.me = strconv.Itoa(nanosecond)+strconv.FormatInt(randNum,10)
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	reqeustCnt := int64(0)
	serverCnt := int64(len(ck.servers))
	requestId := fmt.Sprintf("%v:%v",ck.me,atomic.AddInt64(ck.requestCnt,1))
	ClientPrintf(Info,ck.me,"send a get request requestId=%v",requestId)
	args := &GetArgs{
		Key: key,
		RequestId: requestId,
	}
	reply := &GetReply{}
	for {
		idx := ck.leader
		ck.servers[idx].Call("KVServer.Get",args,reply)
		//不成功就一直重试.
		if reply.Code == SUCCESS{
			atomic.StoreInt64(&ck.leader,idx)
			return reply.Value
		} else if reply.Code == REPEAT_REQUEST{ //目前设计的get不会算作重复请求
			ClientPrintf(Warn, ck.me, "send a repeated get request: reqeustId=%v",requestId)
		}
		reqeustCnt++
		idx = (idx+1)%serverCnt
		if reqeustCnt == serverCnt{
			ClientPrintf(Error,ck.me,"can not find leader: requestId=%v",requestId)
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	reqeustCnt := int64(0)
	serverCnt := int64(len(ck.servers))
	requestId := fmt.Sprintf("%v:%v",ck.me,atomic.AddInt64(ck.requestCnt,1))
	ClientPrintf(Info,ck.me,"send a %v request requestId=%v",op,requestId)
	args := &PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		RequestId: requestId,
	}
	reply := &PutAppendReply{}
	idx := ck.leader
	for {
		ck.servers[idx].Call("KVServer.PutAppend",args,reply)
		//不成功就一直重试.
		if reply.Code == SUCCESS{
			atomic.StoreInt64(&ck.leader,idx)
			return
		} else if reply.Code == REPEAT_REQUEST{ //重复了的putAppend请求, 可能会打印多次...到时候看看是否需要修改下
			ClientPrintf(Warn, ck.me, "send a repeated putAppend request: reqeustId= %v",requestId)
		}
		reqeustCnt++
		idx = (idx+1)%serverCnt
		if reqeustCnt == serverCnt{
			ClientPrintf(Error,ck.me,"can not find leader, request=%v",requestId)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
