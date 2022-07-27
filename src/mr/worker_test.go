package mr

import (
	"fmt"
	"testing"
)

var servant *Servant

func init() {
	servant = MakeServant()
}

func TestServant_Ping(t *testing.T) {
	reply, ok := servant.Ping()
	if ok {
		fmt.Println(reply)
	} else {
		fmt.Println("master已关闭")
	}
}

func TestWorker(t *testing.T) {
	Worker(nil, nil)
}
