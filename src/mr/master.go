package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

type Master struct {
	nReduce              int                   `json:"n_reduce"`                //reduce任务数
	nMap                 int                   `json:"n_map"`                   //map任务数
	ReadyServant         chan string           `json:"ready_servant"`           //准备好入队的servant, 容量暂时设置为100吧, 应该不会超(超了只会影响可用性)
	SaveServant          []string              `json:"save_servant"`            //存活的servant, 只会被HeartCheck(一个携程运行)使用, 不存在并发问题
	DieServant           chan string           `json:"die_servant"`             //本次检测死亡的servant, 也100吧
	ServantStat          sync.Map              `json:"servant_stat"`            //servant的状态, true存活, false死亡
	FinishTask           sync.Map              `json:"finish_task"`             //判断某个任务是否完成, 不区分map/reduce任务, 只是进行部分过滤, 实际上真正的保证幂等操作由FinishXXXCheck保证
	MapTask              []chan *Task          `json:"map_task"`                //map任务, 0是未执行, 1是执行中, 100, 这个超了也没影响, 说明servant不够了
	ReduceTask           []chan *Task          `json:"reduce_task"`             //同上
	MapTaskCnt           int                   `json:"map_task_cnt"`            //map任务数
	ReduceTaskCnt        int                   `json:"reduce_task_cnt"`         //reduce任务数
	FinishMapTaskCnt     int                   `json:"finish_map_task_cnt"`     //已完成的map任务数
	FinishMapTaskChan    chan int              `json:"finish_map_task_chan"`    //维护已完成的map任务数, 防止并发修改对上面的那个出问题, 100, 超了没影响
	FinishMapCheck       map[int]bool          `json:"finish_map_check"`        //用来检查一个Map任务是否被计数
	FinishReduceTaskCnt  int                   `json:"finish_reduce_task_cnt"`  //已完成的reduce任务数, 千万和reduce_id区分开
	FinishReduceTaskChan chan int              `json:"finish_reduce_task_chan"` //维护已完成的Reduce任务数, 100
	FinishReduceCheck    map[int]bool          `json:"finish_reduce_check"`     //检查reduce任务是否被计数
	State                sync.Map              `json:"state"`                   //状态, false为map任务状态,否则是reduce任务状态, 用sync.Map是为了保证线程可见性
	WorkHeartTime        sync.Map              `json:"work_heart_time"`         //Work最近一次发送心跳的时间, ...虽然准确性要求不高(因为会修复,最多HeartTime的误差), 但go里map的并发问题会导致panic...
	ReduceResult         chan *ReduceTaskAck   `json:"reduce_result"`           //reduce任务的返回结果, 1000
	WordCounts           map[string]int        `json:"word_counts"`             //单词数
	ServantTasks         map[string]chan *Task `json:"servant_tasks"`           //每个servant持有的任务
	finish               chan bool             `json:"finish"`                  //是否完成全部任务. 1
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

func (m *Master) RealloTask() {
	for servant := range m.DieServant {
		//目前只有一个, 为了可扩展性还是写成for循环吧
	loop:
		for {
			var task *Task
			select {
			case task = <-m.ServantTasks[servant]:
			default:
				break loop
			}
			fin, ok := m.FinishTask.Load(task.Id)
			if ok && fin.(bool) {
				continue
			}
			if task.MapTask {
				m.MapTask[0] <- task
			} else {
				m.ReduceTask[0] <- task
			}
		}
	}
}

func (m *Master) HeartCheck() {
	currentTime := time.Now().Unix()
	saveServant := make([]string, 0)
	//检测死亡worker改变状态并放入死亡队列
	for _, servant := range m.SaveServant {
		lastHeartTime, ok := m.WorkHeartTime.Load(servant)
		if !ok || currentTime-lastHeartTime.(int64) > int64(time.Millisecond)*HEART_TIME*HEART_CNT {
			m.ServantStat.Store(servant, false)
			m.DieServant <- servant
		} else {
			saveServant = append(saveServant, servant)
		}
	}
	if size := len(m.ReadyServant); size != 0 {
		for i := 0; i < size; i++ {
			select {
			case servant := <-m.ReadyServant:
				saveServant = append(saveServant, servant)
			default:
			}
		}
	}
	m.SaveServant = saveServant
}

//todo: 记个思路
func (m *Master) StateCheck() {
	for id := range m.FinishMapTaskChan {
		if !m.FinishMapCheck[id] {
			m.FinishMapTaskCnt++
			if m.FinishMapTaskCnt == m.MapTaskCnt {
				//处理reduce任务, 删除重复的, 次时ReduceTask中一定已经包含了所有需要的reduce任务
				reduceCnt := 0
				reduceTask := make(chan *Task, len(m.ReduceTask))
				reduceTaskCheck := make(map[int]bool)
			loop:
				for {
					select {
					case task := <-m.ReduceTask[0]:
						if !reduceTaskCheck[task.Id] {
							reduceTaskCheck[task.Id] = true
							reduceCnt++
							reduceTask <- task
						}
					default:
						break loop
					}
				}
				//进入reduce任务处理状态, ReduceTask[0]中可能还有重复任务, 但reduceTaskCnt一定没问题
				m.ReduceTaskCnt = reduceCnt
				m.ReduceTask[0] = reduceTask
				m.State.Store("state", true)
			}
		}
	}
}

func (m *Master) getTaskId(mapId int, reduceId int) int {
	//防止reduceId和MapId重复,MapId范围为0~nMap, reduceId范围为nMap+5~...
	return (mapId+1)*m.nMap + reduceId + 5
}

func (m *Master) MapAck(args *MapTaskAck, reply *bool) error {
	ans := true
	fin, ok := m.FinishTask.Load(args.Id)
	if ok && fin.(bool) {
		reply = &ans
		return nil
	}
	//用sync的Map只是为了防止并发情况导致普通map的panic, 有这样一种情况
	//1. 该任务被重复分配(因为到达RETRY_RATE/误判servant死亡)
	//2. 二者同时MapAck, 然后同时进行了Load都得到false
	//3. 因为Load和Store并非原子操作, 所以会导致两个线程同时通过上面的判断

	//两种解决方法
	//1. 加锁, 影响性能, 简单粗暴
	//2. 保证操作幂等性, 让二者同时执行就行, 因为是根据id进行幂等性判断, 所以id不能自增, 否则发生上述情况后就会得到两个id, 从而使得两个一样的任务被误判为两个
	//因为MapId是串行生成的, 而ReduceId是开始时就确定的, 因此对于Map任务, Id就是MapId, 而reduce任务的一个reduceId会对应多个任务, 但只会对应一个MapId-ReduceId
	//故通过MapId和ReduceId来唯一确定reduce任务的Id
	m.FinishTask.Store(args.Id, true)
	for reduceId, file := range args.ReduceFiles {
		id := m.getTaskId(args.MapId, reduceId)
		task := &Task{
			MapTask:  false,
			Id:       id,
			ReduceId: reduceId,
			FileName: file,
		}
		m.ReduceTask[0] <- task
	}
	m.FinishMapTaskChan <- args.Id
	reply = &ans
	return nil
}

func (m *Master) ReduceAck(args *ReduceTaskAck, reply *bool) error {
	ans := true
	fin, ok := m.FinishTask.Load(args.Id)
	if ok && fin.(bool) {
		reply = &ans
		return nil
	}
	m.ReduceResult <- args
	m.FinishTask.Store(args.Id, true)
	reply = &ans
	return nil
}

func (m *Master) statisticsWords() {
	for reduceTaskAck := range m.ReduceResult {
		if m.FinishReduceCheck[reduceTaskAck.Id] {
			continue
		} else {
			for _, kv := range reduceTaskAck.WordCounts {
				v, _ := strconv.Atoi(kv.Value)
				m.WordCounts[kv.Key] += v
			}
			m.FinishReduceCheck[reduceTaskAck.Id] = true
			m.FinishReduceTaskCnt++
			//写入文件, 然后修改状态
			if m.FinishReduceTaskCnt == m.ReduceTaskCnt {
				file, _ := os.Create("mr-out-0")
				enc := json.NewEncoder(file)
				for key, value := range m.WordCounts {
					keyValue := &KeyValue{
						Key:   key,
						Value: strconv.Itoa(value),
					}
					enc.Encode(keyValue)
				}
				file.Close()
				m.finish <- true
			}
		}
	}
}

func (m *Master) Ping(args *ExampleArgs, reply *ExampleReply) error {
	currentTime := time.Now().Unix()
	//判断该server是否死亡
	ipPort := args.IpPort
	if len(ipPort) == 0 {
		fmt.Errorf("ip_port is empty")
	}
	servantStat := m.ServantStat
	state, ok := servantStat.Load(ipPort)
	//如果是新的或者已死亡的
	if !ok || !state.(bool) {
		//死亡的servant的任务会被重新分配, 给个新的chan
		m.ServantTasks[ipPort] = make(chan *Task, 1)
		servantStat.Store(ipPort, true)
		m.WorkHeartTime.Store(ipPort, currentTime)
		m.ReadyServant <- ipPort
	}

	//看看servant是否空闲
	if args.Free {
		taskChan := m.ReduceTask[0]
		secondChan := m.ReduceTask[1]
		taskCnt := m.ReduceTaskCnt
		finishTaskCnt := m.FinishReduceTaskCnt
		//map任务阶段
		taskState, ok := m.State.Load("state")
		if !ok || !taskState.(bool) {
			taskChan = m.MapTask[0]
			secondChan = m.MapTask[1]
			taskCnt = m.MapTaskCnt
			finishTaskCnt = m.FinishMapTaskCnt
		}
		select {
		case task := <-taskChan:
			reply.HasTask = true
			reply.Task = task
		default:
		}
		if float64(finishTaskCnt)/float64(taskCnt) >= RETRY_RATE {
		loop:
			for {
				select {
				case task := <-secondChan:
					finish, ok := m.FinishTask.Load(task.Id)
					if !ok {
						panic("load task_id err")
					}
					if !finish.(bool) {
						reply.HasTask = true
						reply.Task = task
						break loop
					}
				default:
					break loop
				}
			}
		}
		//如果分配了任务, 则把任务放到处理中队列, 并记录处理它的servant(该servaltG了后重新分配)
		if reply.HasTask {
			secondChan <- reply.Task
			m.ServantTasks[ipPort] <- reply.Task
		}
	}
	reply.HasTask = false
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	//sockname := masterSock()
	//os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	select {
	case <-m.finish:
		ret = true
	default:
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		ReadyServant:         make(chan string, 100),
		DieServant:           make(chan string, 100),
		ReduceTask:           []chan *Task{make(chan *Task, 100), make(chan *Task, 100)},
		MapTask:              []chan *Task{make(chan *Task, 100), make(chan *Task, 100)},
		finish:               make(chan bool, 1),
		FinishMapCheck:       make(map[int]bool, 0),
		FinishReduceCheck:    make(map[int]bool, 0),
		FinishMapTaskChan:    make(chan int, 100),
		FinishReduceTaskChan: make(chan int, 100),
		WordCounts:           make(map[string]int, 0),
		nReduce:              nReduce,
	}
	// Your code here.

	go m.HeartCheck()
	go m.RealloTask()

	m.server()
	return &m
}
