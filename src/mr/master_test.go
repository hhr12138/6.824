package mr

import (
	"testing"
	"time"
)

var master *Master

func init() {
	master = MakeMaster([]string{}, 10)
}

func TestPing(t *testing.T) {

	//空闲无任务
	master.MapTaskCnt = 10
	args := &ExampleArgs{
		Free:   true,
		IpPort: "82.156.8.118:8080",
	}
	reply := &ExampleReply{}
	master.Ping(args, reply)

	//空闲有任务
	master.MapTask[0] <- &Task{
		MapTask:  true,
		Id:       1,
		MapId:    1,
		FileName: "./file/hello.txt",
	}
	master.Ping(args, reply)

	//不空闲
	args.Free = false
	master.Ping(args, reply)
}

//可以更改较长的sleep用来协助worker的Ping测试
func TestDone(t *testing.T) {
	go func() {
		time.Sleep(time.Minute * 5)
		master.finish <- true
	}()
	for !master.Done() {
		//fmt.Println(time.Now())
		time.Sleep(time.Second)
	}
}
