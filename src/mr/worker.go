package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
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
	free := len(s.Free) != 0
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
			//fmt.Printf("ping\n")
			reply, ok := servant.Ping()
			//fmt.Printf("ok\n")
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
				mapTaskAck, err := servant.MapFunc(task, mapf)
				if err == nil {
					ans := &AckReply{}
					call("Master.MapAck", mapTaskAck, ans)
				}
			} else {
				reduceTaskAck, err := servant.ReduceFunc(task, reducef)
				if err == nil {
					ans := &AckReply{}
					call("Master.ReduceAck", reduceTaskAck, ans)
				}
			}
			//必须写到这里, 如果写下面会出现, 心跳线程刚刚拿到任务修改free状态后该线程又给他该回去, 导致错误获取多个任务的情况, 只有处理完任务(无论成功失败)才能算空闲
			//信号量机制保证, 信号量为1
			//也许把请求任务和心跳机制解耦可以不用处理这部分.
			servant.Free <- true
		default:
		}
	}
}

func (s *Servant) MapFunc(task *Task, mapf func(string, string) []KeyValue) (*MapTaskAck, error) {
	id := task.Id
	mapId := task.MapId
	fileName := task.FileName
	bs, err := os.ReadFile(fileName)
	if err != nil {
		fmt.Printf("read file err: %v\n", err)
		return nil, err
	}
	reduceValue := make([][]KeyValue, task.NReduce)
	content := string(bs)
	keyValue := mapf(fileName, content)
	for _, kv := range keyValue {
		key := kv.Key
		reduceId := ihash(key) % task.NReduce
		reduceValue[reduceId] = append(reduceValue[reduceId], kv)
	}
	reduceFiles := make([]string, task.NReduce)
	_, err = os.Stat(DIR_PATH)
	if os.IsNotExist(err) {
		err = os.MkdirAll(DIR_PATH, os.ModePerm)
		if err != nil {
			fmt.Printf("creat dir fail: err = %v\n", err.Error())
			return nil, err
		}
	}
	for idx, kvs := range reduceValue {
		file, err := ioutil.TempFile(DIR_PATH, "*.txt")
		if err != nil {
			fmt.Printf("create file err: %v\n", err.Error())
			return nil, err
		}
		enc := json.NewEncoder(file)
		for _, kv := range kvs {
			enc.Encode(kv)
		}
		reduceFiles[idx] = file.Name()
		file.Close()
	}
	ans := &MapTaskAck{
		Id:          id,
		MapId:       mapId,
		ReduceFiles: reduceFiles,
	}
	return ans, nil
}

func (s *Servant) ReduceFunc(task *Task, reducef func(string, []string) string) (*ReduceTaskAck, error) {
	id := task.Id
	reduceId := task.ReduceId
	mp := make(map[string][]string, 0)
	for _, fileName := range task.Files {
		file, err := os.Open(DIR_PATH + fileName)
		if err != nil {
			fmt.Printf("open file err: %v\n", err.Error())
			return nil, err
		}
		doc := json.NewDecoder(file)
		for {
			var kv KeyValue
			err = doc.Decode(&kv)
			if err != nil {
				break
			}
			mp[kv.Key] = append(mp[kv.Key], kv.Value)
		}
	}
	kv := make([]KeyValue, 0)
	for key, strs := range mp {
		value := reducef(key, strs)
		keyVale := KeyValue{
			Key:   key,
			Value: value,
		}
		kv = append(kv, keyVale)
	}

	reduceTaskAck := &ReduceTaskAck{
		ReduceId:   reduceId,
		Id:         id,
		WordCounts: kv,
	}
	return reduceTaskAck, nil
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
