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

//36:10~36:32
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

func (ck *Clerk) getRequestId() string{
	requestId := fmt.Sprintf("%v:%v",ck.me,atomic.AddInt64(ck.requestCnt,1))
	return requestId
}

func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	result := ""
	reqeustCnt := int64(0)
	serverCnt := int64(len(ck.servers))
	requestId := ck.getRequestId()
	args := &GetArgs{
		Key: key,
		RequestId: requestId,
	}
	reply := &GetReply{}
	idx := atomic.LoadInt64(&ck.leader)
	for {
		args.RequestCnt = reqeustCnt
		ClientPrintf(Info,ck.me,"send a get request requestId=%v, requestCnt=%v",requestId,reqeustCnt)
		ok := ck.servers[idx].Call("KVServer.Get", args, reply)
		ClientPrintf(Info, ck.me, "asdoipfuasdigyurieqjt;oyi requestId=%v, requestCnt=%v",requestId, reqeustCnt)
		//不成功就一直重试.
		if ok && reply.Code == SUCCESS{
			atomic.StoreInt64(&ck.leader,idx)
			result = reply.Value
			ClientPrintf(Info,ck.me,"get a get request reply, requestId=%v",requestId)
			break
		} else if reply.Code == REPEAT_REQUEST{ //目前设计的get不会算作重复请求
			ClientPrintf(Warn, ck.me, "send a repeated get request: reqeustId=%v",requestId)
		} else{
			ClientPrintf(Info, ck.me, "%v is not leader",idx)
		}
		reqeustCnt++
		idx = (idx+1)%serverCnt
		if reqeustCnt == serverCnt{
			ClientPrintf(Error,ck.me,"can not find leader: requestId=%v",requestId)
		}
		time.Sleep(CLIENT_SLEEP_TIME*time.Millisecond)
	}
	//ck.Remove(requestId)
	ClientPrintf(Critcal,ck.me,"get request return requestId=%v, key=%v, value=%v",requestId, key,result)
	return result
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
	requestId := ck.getRequestId()
	ClientPrintf(Info,ck.me,"send a %v request requestId=%v",op,requestId)
	args := &PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		RequestId: requestId,
	}
	reply := &PutAppendReply{}
	idx := atomic.LoadInt64(&ck.leader)
	for {
		ClientPrintf(Info,ck.me,"send a %v request requestId=%v, requestCnt=%v",op,requestId,reqeustCnt)
		args.RequestCnt = reqeustCnt
		ck.servers[idx].Call("KVServer.PutAppend",args,reply)
		ClientPrintf(Info,ck.me,"send a %v request requestId=%v, requestCnt=%v finish",op,requestId,reqeustCnt)
		//不成功就一直重试.
		if reply.Code == SUCCESS{
			atomic.StoreInt64(&ck.leader,idx)
			ClientPrintf(Info,ck.me,"get a %v request reply, requestId=%v",op,requestId)
			break
		} else if reply.Code == REPEAT_REQUEST{ //重复了的putAppend请求, 可能会打印多次...到时候看看是否需要修改下
			ClientPrintf(Warn, ck.me, "send a repeated putAppend request: reqeustId= %v",requestId)
		} else{
			ClientPrintf(Info, ck.me, "%v is not leader",idx)
		}
		reqeustCnt++
		idx = (idx+1)%serverCnt
		if reqeustCnt == serverCnt{
			ClientPrintf(Error,ck.me,"can not find leader, request=%v",requestId)
		}
		time.Sleep(CLIENT_SLEEP_TIME*time.Millisecond)
	}
	//ck.Remove(requestId)
	ClientPrintf(Info,ck.me,"get/append success requestId=%v",requestId)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) Remove(removeRequestId string){
	defer ClientPrintf(Info,ck.me,"remove success")
	requestCnt := int64(0)
	requestId := ck.getRequestId()
	serverCnt := int64(len(ck.servers))
	args := &RemoveArgs{
		RequestId: requestId,
		RemoveRequestId: removeRequestId,
	}
	ClientPrintf(Info, ck.me, "send a remove reqeust, requestId=%v",requestId)
	reply := &RemoveReply{}
	idx := atomic.LoadInt64(&ck.leader)
	for {
		ClientPrintf(Info,ck.me,"send a remove request requestId=%v",requestId)
		args.RequestCnt = requestCnt
		ck.servers[idx].Call("KVServer.Remove",args,reply)
		//不成功就一直重试.
		if reply.Code == SUCCESS{
			atomic.StoreInt64(&ck.leader,idx)
			ClientPrintf(Info, ck.me, "get a remove reqeust reply, requestId=%v", requestId)
			//break
		} else if reply.Code == REPEAT_REQUEST{ //重复了的putAppend请求, 可能会打印多次...到时候看看是否需要修改下
			ClientPrintf(Warn, ck.me, "send a repeated putAppend request: reqeustId= %v",requestId)
		} else{
			ClientPrintf(Info, ck.me, "%v is not leader",idx)
		}
		requestCnt++
		idx = (idx+1)%serverCnt
		if requestCnt == serverCnt{
			ClientPrintf(Error,ck.me,"can not find leader, request=%v",requestId)
		}
	}
	ClientPrintf(Info, ck.me, "remove success requetId=%v",requestId)
}