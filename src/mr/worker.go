package mr

import (
	"fmt"
	"os"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type Servant struct {
	IPPort      string              `json:"ip_port"`      //该Servant的唯一标识
	Tasks       chan *Task          `json:"tasks"`        //理论上只会有一个, 1
	ReduceValue []string            `json:"reduce_value"` //idx为reduce_id, string为K V形式, 代表着一个KEYVALUE对, map任务用到
	KeyValues   map[string][]string `json:"key_values"`   //reduce任务用到的KeyValues, 见Worker
	finish      chan bool           `json:"finish"`       //是否结束, 1
	Free        chan bool           `json:"free"`         //是否空闲, go没有volatile...只能这么实现
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (s *Servant) Done() bool {
	select {
	case <-s.finish:
		return true
	default:
		return false
	}
}

func (s *Servant) Ping() (*ExampleReply, bool) {
	free := false
	select {
	case <-s.Free:
		free = true
	default:
	}
	args := &ExampleArgs{
		IpPort: s.IPPort,
		Free:   free,
	}
	reply := &ExampleReply{}
	if call("Master.Ping", args, reply) {
		return reply, true
	}
	return nil, false
}

func MakeServant() *Servant {
	servant := &Servant{
		Tasks:       make(chan *Task, 1),
		IPPort:      strconv.Itoa(os.Getpid()), //因为是在本机运行, 所以用pid来充当ipPort当作唯一标识
		ReduceValue: make([]string, 0),
		KeyValues:   make(map[string][]string, 0),
		finish:      make(chan bool, 1),
		Free:        make(chan bool, 1),
	}
	//开始的时候是空闲的
	servant.Free <- true
	return servant
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	servant := MakeServant()
	//定时心跳
	go func() {
		for range time.Tick(time.Millisecond * HEART_TIME) {
			reply, ok := servant.Ping()
			//没ping通master, 简单认为master已经结束任务, 退出
			if !ok {
				servant.finish <- true
				return
			}
			//有任务就分配
			if reply != nil && reply.HasTask {
				servant.Tasks <- reply.Task
				<-servant.Free
			}
		}
	}()
	// uncomment to send the Example RPC to the master.
	// CallExample()
	for range time.Tick(time.Millisecond * CHECK_TIME) {
		if servant.Done() {
			break
		}
		select {
		case task := <-servant.Tasks:
			if task == nil {
				break
			}
			//写到这里了, 继续吧
			if task.MapTask {
				MapFunc()
			} else {
				ReduceFunc()
			}
		default:
		}
	}
}

func MapFunc() {

}

func ReduceFunc() {

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func CallExample() {
//
//	// declare an argument structure.
//	args := ExampleArgs{}
//
//	// fill in the argument(s).
//	args.X = 99
//
//	// declare a reply structure.
//	reply := ExampleReply{}
//
//	// send the RPC request, wait for the reply.
//	call("Master.Example", &args, &reply)
//
//	// reply.Y should be 100.
//	fmt.Printf("reply.Y %v\n", reply.Y)
//}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//sockname := masterSock()
	//c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
