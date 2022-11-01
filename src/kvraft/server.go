package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const IsNotDebug = 0
const nowLogLevel = Info

type LogLevel int

const (
	Debug   LogLevel = 0
	Info    LogLevel = 1
	Warn    LogLevel = 2
	Error   LogLevel = 3
	Critcal LogLevel = 4
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if IsNotDebug > 0 {
		log.Printf(format, a...)
	}
	return
}

func MyPrintf(level LogLevel, me int, format string, a ...interface{}) string {
	if level < nowLogLevel {
		return ""
	}
	str := fmt.Sprintf("level=%v, server %v ", level, me)
	ans := fmt.Sprintf(str+format, a...)
	DPrintf(str+format, a...)
	return ans
}
func ClientPrintf(level LogLevel, me string, format string, a ...interface{}) string {
	if level < nowLogLevel {
		return ""
	}
	str := fmt.Sprintf("level=%v, server %v ", level, me)
	ans := fmt.Sprintf(str+format, a...)
	DPrintf(str+format, a...)
	return ans
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type LogState int

//type LogCache struct{
//	state LogState
//	value string
//}
//
//const(
//	NOT_FIND LogState = 0
//	COMMITTED LogState = 1
//	WORKING LogState = 2
//)

type SnapshotNode struct {
	Database  map[string]string //KV数据库
	LogStates map[string]string //记录每个log的返回值
}

type KVServer struct {
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	dead     int32             // set by Kill()
	database map[string]string //KV数据库
	//commandToResp map[string] chan string//用来兼容请求响应模型和流式处理模型的map, term:index->该log的执行结果
	logStates map[string]string //记录每个log的返回值
	//logStates    sync.Map
	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister
	// Your definitions here.
}

func (kv *KVServer) sendRequest(common *raft.LogCommand) (Code, string, string) {
	marshal, _ := json.Marshal(common)
	//start成功后raft立即开始执行, 如果在raft执行完成并返回结构后commandToResp都没set就会导致execute方法空指针, 因此commandToResp也可以换成普通map了
	requestId := common.RequestId
	_, term, isLeader := kv.rf.Start(marshal)
	if kv.killed() || !isLeader {
		//kv.mu.Unlock()
		return NOT_LEADER, "", "is not leader"
	}
	for {
		nowTerm, isLeader := kv.rf.GetState()
		if nowTerm != term || !isLeader {
			MyPrintf(Error, kv.me, "term change, term=%v,nowTerm=%v", term, nowTerm)
			return NOT_LEADER, "", "is not leader"
		}
		kv.mu.Lock()
		cache, exist := kv.logStates[requestId]
		//cache, exist := kv.logStates.Load(requestId)
		if exist {
			kv.mu.Unlock()
			return SUCCESS, cache, ""
		}
		kv.mu.Unlock()
		time.Sleep(WAIT_CHANNEL_RESP_SLEEP_TIME * time.Millisecond)
	}
}

//remove是幂等的, 因为requestId唯一,对于一个requestId删除几次都无所谓
func (kv *KVServer) Remove(args *RemoveArgs, reply *RemoveReply) {
	var common = &raft.LogCommand{
		RequestId: args.RequestId,
		IsGet:     true,
		Command: raft.Command{
			Ope: "Remove",
			Key: args.RemoveRequestId,
		},
	}
	code, _, err := kv.sendRequest(common)
	//kv.mu.Lock()
	//delete(kv.logStates,args.RequestId)
	//kv.mu.Unlock()
	reply.Code = code
	reply.Err = err
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//get请求保证幂等了, 无需记录id
	common := &raft.LogCommand{
		RequestId: args.RequestId,
		IsGet:     true,
		Command: raft.Command{
			Ope: "Get",
			Key: args.Key,
		},
	}
	code, val, err := kv.sendRequest(common)
	reply.Err = Err(err)
	reply.Value = val
	reply.Code = code
}

//可以尝试加个超时时间
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	common := &raft.LogCommand{
		RequestId: args.RequestId,
		IsGet:     false,
		Command: raft.Command{
			Ope:   args.Op,
			Key:   args.Key,
			Value: args.Value,
		},
	}
	code, _, err := kv.sendRequest(common)
	reply.Code = code
	reply.Err = Err(err)
}

func (kv *KVServer) executeLogs() {
	for {
		msg := <-kv.applyCh
		MyPrintf(Info, kv.me, "get a msg: msgIndex=%v", msg.CommandIndex)
		kv.mu.Lock()
		MyPrintf(Info, kv.me, "msg into lock: msgIndex=%v", msg.CommandIndex)
		if kv.killed() {
			MyPrintf(Error, kv.me, "is killed")
			kv.mu.Unlock()
			return
		}
		if msg.CommandValid {
			command := &raft.LogCommand{}
			bytes, ok := msg.Command.([]byte)
			if !ok {
				MyPrintf(Error, kv.me, "can not change command to []byte command=%v", command)
			}
			err := json.Unmarshal(bytes, command)
			if err != nil {
				MyPrintf(Error, kv.me, "can not numarshal bytes to raft.LogCommand bytes=%v", bytes)
			}
			requestId := command.RequestId
			key := command.Key
			switch command.Ope {
			case "Get":
				if _, exist := kv.logStates[requestId]; !exist {
					//if _, exist := kv.logStates.Load(requestId); !exist {
					value := kv.database[key]
					//targetVal = value
					kv.logStates[requestId] = value
					//kv.logStates.Store(requestId, value)
					MyPrintf(Info, kv.me, "get request success requestId=%v, key=%v, value=%v, index=%v", requestId, key, value, msg.CommandIndex)
				}
			case "Put":
				if _, exist := kv.logStates[requestId]; !exist {
					//if _, exist := kv.logStates.Load(requestId); !exist {
					value := command.Value
					kv.database[key] = value
					kv.logStates[requestId] = "success"
					//kv.logStates.Store(requestId, "success")
					//targetVal = "success"
					MyPrintf(Info, kv.me, "Put request success requestId=%v, key=%v, value=%v, index=%v", requestId, key, value, msg.CommandIndex)
				}
			case "Append":
				if _, exist := kv.logStates[requestId]; !exist {
					//if _, exist := kv.logStates.Load(requestId); !exist {
					value := command.Value
					val := kv.database[key]
					val += value
					kv.database[key] = val
					kv.logStates[requestId] = "success"
					MyPrintf(Info, kv.me, "Append request success requestId=%v, key=%v, value=%v, index=%v", requestId, key, value, msg.CommandIndex)
					//kv.logStates.Store(requestId, "success")
					//targetVal = "success"
				}
			case "Remove":
				delete(kv.logStates, key)
				//kv.logStates.Delete(key)
				//targetVal = "success"
				kv.logStates[requestId] = "success"
				//kv.logStates.Store(requestId, "success")
				MyPrintf(Info, kv.me, "Remove request success requestId=%v, key=%v, value=%v, index=%v", requestId, key, msg.CommandIndex)
			default:
				MyPrintf(Critcal, kv.me, "undefined ope")
				panic("undefined ope")
			}
			//kv.mu.Unlock()
		}
		// 说实话, 这里用子进程去做更好, 快照阻塞服务可不是什么理想的事情, 不过是个case就算了.
		size := kv.persister.RaftStateSize()
		if kv.maxraftstate != -1 && size >= kv.maxraftstate-BUFFER_SIZE {
			snapshotNode := SnapshotNode{
				Database:  kv.database,
				LogStates: kv.logStates,
			}
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(snapshotNode)
			bytes := w.Bytes()
			kv.rf.Snapshot(msg.CommandIndex, bytes)
		}
		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	MyPrintf(Info, kv.me, "died")
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) loadSnapshot(bs []byte) {
	if bs == nil || len(bs) == 0 {
		return
	}
	snapshot := &raft.Snapshot{}
	r := bytes.NewBuffer(bs)
	decoder := labgob.NewDecoder(r)
	if err := decoder.Decode(snapshot); err != nil {
		MyPrintf(Error, kv.me, "server loadSnapshot decode snapshot err, err=%v", err.Error())
	}
	if snapshot.SnapshotBytes == nil || len(snapshot.SnapshotBytes) == 0 {
		return
	}
	snapshotNode := &SnapshotNode{}
	r = bytes.NewBuffer(snapshot.SnapshotBytes)
	decoder = labgob.NewDecoder(r)
	if err := decoder.Decode(snapshotNode); err != nil {
		MyPrintf(Error, kv.me, "server loadSnapshot decode snapshotNode err, err=%v", err.Error())
	}
	kv.database = snapshotNode.Database
	kv.logStates = snapshotNode.LogStates
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	kv := new(KVServer)
	kv.me = me
	kv.persister = persister
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.logStates = make(map[string]string, 0)
	kv.database = make(map[string]string, 0)
	//kv.commandToResp = make(map[string] chan string)
	kv.loadSnapshot(persister.ReadSnapshot())
	// You may need initialization code here.
	go kv.executeLogs()
	return kv
}
