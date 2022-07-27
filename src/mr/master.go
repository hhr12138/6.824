package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

type Master struct {
	ReadyServant         chan string           `json:"ready_servant"`           //准备好入队的servant, 容量暂时设置为100吧, 应该不会超(超了只会影响可用性)
	SaveServant          []string              `json:"save_servant"`            //存活的servant, 只会被HeartCheck(一个携程运行)使用, 不存在并发问题
	DieServant           chan string           `json:"die_servant"`             //本次检测死亡的servant, 也100吧
	ServantStat          sync.Map              `json:"servant_stat"`            //servant的状态, true存活, false死亡
	FinishTask           sync.Map              `json:"finish_task"`             //判断某个任务是否完成, 不区分map/reduce任务
	MapTask              []chan *Task          `json:"map_task"`                //map任务, 0是未执行, 1是执行中, 100, 这个超了也没影响, 说明servant不够了
	ReduceTask           []chan *Task          `json:"reduce_task"`             //同上
	MapTaskCnt           int                   `json:"map_task_cnt"`            //map任务数
	ReduceTaskCnt        int                   `json:"reduce_task_cnt"`         //reduce任务数
	FinishMapTaskCnt     int                   `json:"finish_map_task_cnt"`     //已完成的map任务数
	FinishMapTaskChan    chan bool             `json:"finish_map_task_chan"`    //维护已完成的map任务数, 防止并发修改对上面的那个出问题, 100, 超了没影响
	FinishReduceTaskCnt  int                   `json:"finish_reduce_task_cnt"`  //已完成的reduce任务数, 千万和reduce_id区分开
	FinishReduceTaskChan chan bool             `json:"finish_reduce_task_chan"` //维护已完成的Reduce任务数, 100
	State                bool                  `json:"state"`                   //状态, true为map任务状态,否则是reduce任务状态
	WorkHeartTime        sync.Map              `json:"work_heart_time"`         //Work最近一次发送心跳的时间, ...虽然准确性要求不高(因为会修复,最多HeartTime的误差), 但go里map的并发问题会导致panic...
	ReduceResult         chan []KeyValue       `json:"reduce_result"`           //reduce任务的返回结果, 100
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
		if m.State {
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
		ReadyServant: make(chan string, 100),
		DieServant:   make(chan string, 100),
		ReduceTask:   []chan *Task{make(chan *Task, 100), make(chan *Task, 100)},
		MapTask:      []chan *Task{make(chan *Task, 100), make(chan *Task, 100)},
		State:        true,
		finish:       make(chan bool, 1),
	}
	// Your code here.

	m.server()
	return &m
}
