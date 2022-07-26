package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Master struct {
	nReduce             int                   `json:"n_reduce"`               //reduce任务数
	nMap                int                   `json:"n_map"`                  //map任务数
	ReadyServant        chan string           `json:"ready_servant"`          //准备好入队的servant, 容量暂时设置为100吧, 应该不会超(超了只会影响可用性)
	SaveServant         []string              `json:"save_servant"`           //存活的servant, 只会被HeartCheck(一个携程运行)使用, 不存在并发问题
	DieServant          chan string           `json:"die_servant"`            //本次检测死亡的servant, 也100吧
	ServantStat         sync.Map              `json:"servant_stat"`           //servant的状态, true存活, false死亡
	FinishTask          sync.Map              `json:"finish_task"`            //判断某个任务是否完成, 不区分map/reduce任务, 只是进行部分过滤, 实际上真正的保证幂等操作由FinishXXXCheck保证
	MapTask             []chan *Task          `json:"map_task"`               //map任务, 0是未执行, 1是执行中, 100, 这个超了也没影响, 说明servant不够了
	ReduceTask          []chan *Task          `json:"reduce_task"`            //同上
	MapTaskCnt          int                   `json:"map_task_cnt"`           //map任务数
	ReduceTaskCnt       int                   `json:"reduce_task_cnt"`        //reduce任务数
	FinishMapTaskCnt    int                   `json:"finish_map_task_cnt"`    //已完成的map任务数
	FinishMapTaskChan   chan int              `json:"finish_map_task_chan"`   //维护已完成的map任务数, 防止并发修改对上面的那个出问题, 100, 超了没影响
	FinishMapCheck      map[int]bool          `json:"finish_map_check"`       //用来检查一个Map任务是否被计数
	FinishReduceTaskCnt int                   `json:"finish_reduce_task_cnt"` //已完成的reduce任务数, 千万和reduce_id区分开
	FinishReduceCheck   map[int]bool          `json:"finish_reduce_check"`    //检查reduce任务是否被计数
	State               sync.Map              `json:"state"`                  //状态, false为map任务状态,否则是reduce任务状态, 用sync.Map是为了保证线程可见性
	WorkHeartTime       sync.Map              `json:"work_heart_time"`        //Work最近一次发送心跳的时间, ...虽然准确性要求不高(因为会修复,最多HeartTime的误差), 但go里map的并发问题会导致panic...
	ReduceResult        chan *ReduceTaskAck   `json:"reduce_result"`          //reduce任务的返回结果, 100
	WordCounts          map[string]int        `json:"word_counts"`            //单词数
	ServantTasks        map[string]chan *Task `json:"servant_tasks"`          //每个servant持有的任务
	finish              chan bool             `json:"finish"`                 //是否完成全部任务. 1
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
	for range time.Tick(time.Millisecond * HEART_TIME) {
		currentTime := time.Now().UnixNano() / 1000000
		saveServant := make([]string, 0)
		//检测死亡worker改变状态并放入死亡队列
		for _, servant := range m.SaveServant {
			lastHeartTime, ok := m.WorkHeartTime.Load(servant)
			if !ok || currentTime-lastHeartTime.(int64) > HEART_TIME*HEART_CNT {
				fmt.Println("servant: " + servant + "died")
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
}

//todo: 记个思路
func (m *Master) StateCheck() {
	for id := range m.FinishMapTaskChan {
		if !m.FinishMapCheck[id] {
			m.FinishMapCheck[id] = true
			m.FinishMapTaskCnt++
			if m.FinishMapTaskCnt == m.MapTaskCnt {
				//处理reduce任务, 删除重复的, 次时ReduceTask中一定已经包含了所有需要的reduce任务
				reduceCnt := 0
				reduceTask := make(chan *Task, m.nReduce)
				reduceTaskCheck := make(map[int]bool)
				taskChange := make(map[int][]string)
			loop:
				for {
					select {
					case task := <-m.ReduceTask[0]:
						if !reduceTaskCheck[task.Id] {
							reduceTaskCheck[task.Id] = true
							taskChange[task.ReduceId] = append(taskChange[task.ReduceId], task.FileName)
						} else {
							fmt.Println("task: " + strconv.Itoa(task.Id) + "already, filename=" + task.FileName)
						}
					default:
						break loop
					}
				}
				for reduceId, reduceFiles := range taskChange {
					task := &Task{
						MapTask:  false,
						Id:       m.nMap + reduceId,
						ReduceId: reduceId,
						Files:    reduceFiles,
					}
					reduceCnt++
					reduceTask <- task
				}
				//进入reduce任务处理状态, ReduceTask[0]中可能还有重复任务, 但reduceTaskCnt一定没问题
				m.ReduceTaskCnt = reduceCnt
				m.ReduceTask[0] = reduceTask
				m.State.Store("state", true)
				return
			}
		}
	}
}

//用来生成一个临时id, 记录一个临时任务是否被多次处理
func (m *Master) GetTaskId(mapId int, reduceId int) int {
	//防止reduceId和MapId重复,MapId范围为0~nMap, reduceId范围为nMap+5~...
	return mapId*m.nReduce + reduceId + m.nMap
}

func (m *Master) MapAck(args *MapTaskAck, reply *AckReply) error {
	fin, ok := m.FinishTask.Load(args.Id)
	if ok && fin.(bool) {
		reply.Success = true
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
	//fmt.Printf("task: " + strconv.Itoa(args.Id) + "done\n")
	for reduceId, file := range args.ReduceFiles {
		id := m.GetTaskId(args.MapId, reduceId)
		fileName := fmt.Sprintf("mr-%v-%v", args.MapId, reduceId)
		os.Rename(DIR_PATH+file, DIR_PATH+fileName)
		task := &Task{
			MapTask:  false,
			Id:       id,
			ReduceId: reduceId,
			FileName: fileName,
			NReduce:  m.nReduce,
		}
		m.ReduceTask[0] <- task
	}
	m.FinishMapTaskChan <- args.Id
	reply.Success = true
	return nil
}

func (m *Master) ReduceAck(args *ReduceTaskAck, reply *AckReply) error {
	fin, ok := m.FinishTask.Load(args.Id)
	if ok && fin.(bool) {
		reply.Success = true
		return nil
	}
	m.ReduceResult <- args
	m.FinishTask.Store(args.Id, true)
	//fmt.Printf("task: " + strconv.Itoa(args.Id) + "done\n")
	reply.Success = true
	return nil
}

func (m *Master) statisticsWords() {
	for reduceTaskAck := range m.ReduceResult {
		if m.FinishReduceCheck[reduceTaskAck.Id] {
			continue
		} else {
			m.FinishReduceCheck[reduceTaskAck.Id] = true

			file, _ := os.OpenFile("mr-out-0", os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModePerm)
			for _, kv := range reduceTaskAck.WordCounts {
				_, err := file.WriteString(fmt.Sprintf("%v %v\n", kv.Key, kv.Value))
				if err != nil {
					fmt.Printf("write err=%v\n", err)
				}
			}
			file.Close()
			fmt.Println("task" + strconv.Itoa(reduceTaskAck.Id) + " done")
			m.FinishReduceTaskCnt++
			if m.FinishReduceTaskCnt == m.ReduceTaskCnt {
				m.finish <- true
				return
			}
		}
	}
}

func (m *Master) Ping(args *ExampleArgs, reply *ExampleReply) error {
	currentTime := time.Now().UnixNano() / 1000000
	//判断该server是否死亡
	ipPort := args.IpPort
	if len(ipPort) == 0 {
		fmt.Println("ip_port is empty")
	}
	m.WorkHeartTime.Store(ipPort, currentTime)
	//如果是新的或者已死亡的
	state, ok := m.ServantStat.Load(ipPort)
	if !ok || !state.(bool) {
		fmt.Println("servant insert")
		//死亡的servant的任务会被重新分配, 给个新的chan
		m.ServantTasks[ipPort] = make(chan *Task, 1)
		m.ServantStat.Store(ipPort, true)
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
		if !reply.HasTask && float64(finishTaskCnt)/float64(taskCnt) >= RETRY_RATE {
			//if !reply.HasTask {
		loop:
			for {
				select {
				case task := <-secondChan:
					finish, ok := m.FinishTask.Load(task.Id)
					if !ok || !finish.(bool) {
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
			select {
			case <-m.ServantTasks[ipPort]:
			default:
			}
			//理论上这步不会阻塞, 因为servant的free一定正确, ps: 这一步是为了防止取出任务后同一个servant发起两个ping导致阻塞, 但理论上不会
			select {
			case m.ServantTasks[ipPort] <- reply.Task:
			default:
				fmt.Println("servant tasks dieLock")
			}
		}
	}
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
		ReadyServant:      make(chan string, nReduce+100),
		DieServant:        make(chan string, nReduce+100),
		ReduceTask:        []chan *Task{make(chan *Task, len(files)*nReduce+100), make(chan *Task, len(files)*nReduce+100)},
		MapTask:           []chan *Task{make(chan *Task, len(files)+100), make(chan *Task, len(files)+100)},
		finish:            make(chan bool, 1),
		FinishMapCheck:    make(map[int]bool, 0),
		FinishReduceCheck: make(map[int]bool, 0),
		FinishMapTaskChan: make(chan int, len(files)+100),
		WordCounts:        make(map[string]int, 0),
		ServantTasks:      make(map[string]chan *Task, 0),
		ReduceResult:      make(chan *ReduceTaskAck, len(files)*nReduce+100),
		SaveServant:       make([]string, 0),
		nReduce:           nReduce,
		nMap:              len(files),
		MapTaskCnt:        len(files),
	}
	// Your code here.
	//创建Map任务, 提前确定好编号
	if m.MapTaskCnt == 0 {
		m.finish <- true
		return &m
	}
	go func() {
		for idx, fileName := range files {
			task := &Task{
				MapTask:  true,
				Id:       idx,
				MapId:    idx,
				NReduce:  nReduce,
				FileName: fileName,
			}
			m.MapTask[0] <- task
		}
	}()
	go m.HeartCheck()
	go m.RealloTask()
	go m.statisticsWords()
	go m.StateCheck()
	m.server()
	return &m
}
