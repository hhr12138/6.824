package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const IsNotDebug = 1
const nowLogLevel = Info
type LogLevel int
const (
	Debug LogLevel = 0
	Info LogLevel = 1
	Warn LogLevel = 2
	Error LogLevel = 3
	Critcal LogLevel = 4
)
const(
	WAIT_CHANNEL_RESP_SLEEP_TIME = 50 //ms
)
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if IsNotDebug > 0 {
		log.Printf(format, a...)
	}
	return
}

func MyPrintf(level LogLevel, me int, format string, a ...interface{}) string {
	if level < nowLogLevel{
		return ""
	}
	str := fmt.Sprintf("level=%v, server %v ",level, me)
	ans := fmt.Sprintf(str+format, a...)
	DPrintf(str+format, a...)
	return ans
}
func ClientPrintf(level LogLevel, me string, format string, a ...interface{}) string {
	if level < nowLogLevel{
		return ""
	}
	str := fmt.Sprintf("level=%v, server %v ",level, me)
	ans := fmt.Sprintf(str+format, a...)
	DPrintf(str+format, a...)
	return ans
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	database map[string]string //KV数据库
	commandToResp sync.Map //用来兼容请求响应模型和流式处理模型的map, term:index->该log的执行结果

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
}

type GetCommand struct{
	Ope string `json:"ope"`
	Args *GetArgs `json:"args"`
}

type PutCommand struct{
	Ope string `json:"ope"`
	Args *PutAppendArgs
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//get请求保证幂等了, 无需记录id
	common := &raft.LogCommand{
		RequestId: args.RequestId,
		IsGet: true,
		Command: GetCommand{
			Ope: "Get",
			Args: args,
		},
	}
	_, _, isLeader := kv.rf.Start(common)
	if kv.killed() || !isLeader{
		reply.Err = "is not leader"
		reply.Code = NOT_LEADER
	} else{
		chId := args.RequestId
		ch := make(chan string,0)
		kv.commandToResp.Store(chId, ch)
		loop:
		for{
			_, isLeader := kv.rf.GetState()
			if !isLeader{
				reply.Err = "is not leader"
				break
			}
			select{
			case reply.Value = <-ch:
				reply.Code = SUCCESS
				break loop
			default:
			}
			time.Sleep(WAIT_CHANNEL_RESP_SLEEP_TIME*time.Millisecond)
		}
	}
	return
}
//可以尝试加个超时时间
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	common := &raft.LogCommand{
		RequestId: args.RequestId,
		IsGet: false,
		Command: PutCommand{
			Ope: args.Op,
			Args: args,
		},
	}
	index, term, isLeader := kv.rf.Start(common)
	if kv.killed() || !isLeader{
		reply.Err = "is not leader"
		reply.Code = NOT_LEADER
	}else if index == -1 || term == -1{
		reply.Err = "repeat request"
		reply.Code = REPEAT_REQUEST
	} else{
		chId := args.RequestId
		ch := make(chan error,0)
		kv.commandToResp.Store(chId, ch)
		loop:
		for{
			select{
			case err := <-ch:
				if err != nil{
					reply.Code = SUCCESS
					reply.Err= Err(err.Error())
				}
				break loop
			default:
			}
		}
		time.Sleep(WAIT_CHANNEL_RESP_SLEEP_TIME*time.Millisecond)
	}
	return
}

func (kv *KVServer) executeLogs(){
	for {
		select {
		case msg:=<-kv.applyCh:
			if msg.CommandValid{
				command, ok := msg.Command.(raft.LogCommand)
				if !ok{
					MyPrintf(Error,kv.me,"can not change command to raft.LogCommand command=%v",command)
				}
				if command.IsGet{
					getCommand := command.Command.(GetCommand)
					key := getCommand.Args.Key
					value := kv.database[key]
					requestId := command.RequestId
					o,_ := kv.commandToResp.Load(requestId)
					ch := o.(chan string)
					ch<-value
					MyPrintf(Info,kv.me,"get request success requestId=%v, key=%v",requestId,key)
				} else{
					putOrAppCommand := command.Command.(PutCommand)
					requestId := putOrAppCommand.Args.RequestId
					op := putOrAppCommand.Args.Op
					key := putOrAppCommand.Args.Key
					value := putOrAppCommand.Args.Value
					if op == "Put"{
						kv.database[key] = value
					} else{
						str := kv.database[key]
						kv.database[key] = str+value
					}
					o,_ := kv.commandToResp.Load(requestId)
					ch := o.(chan error)
					ch<-nil
					MyPrintf(Info,kv.me,"%v success requestId=%v, key=%v, value=%v",op,requestId,key,value)
				}
			}
		}
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
	MyPrintf(Info, kv.me,"died")
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.executeLogs()
	return kv
}
