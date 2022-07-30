package mr

import (
	"io/ioutil"
	"strings"
	"testing"
	"time"
)

var master *Master

func init() {
	files := make([]string, 0)
	path := "../main/"
	dir, _ := ioutil.ReadDir(path)
	for _, file := range dir {
		if !file.IsDir() && strings.Index(file.Name(), "pg") >= 0 {
			files = append(files, path+file.Name())
		}
	}
	master = MakeMaster(files, 10)
}

func TestPing(t *testing.T) {

	//空闲无任务
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

////在RealloTask上打个断点一起测了
//func TestMaster_HeartCheck(t *testing.T) {
//	servantName := "1234"
//	args := &ExampleArgs{
//		Free:   true,
//		IpPort: servantName,
//	}
//	reply := &ExampleReply{}
//	currentTime := time.Now().UnixNano()/1000
//	master.SaveServant = append(master.SaveServant, servantName)
//	master.WorkHeartTime.Store(servantName,currentTime)
//	master.HeartCheck()
//	time.Sleep(time.Second)
//	master.HeartCheck()
//	master.Ping(args,reply)
//	master.HeartCheck()
//}

func TestMaster(t *testing.T) {
	for range time.Tick(time.Second) {
		if master.Done() {
			break
		}
	}
}
