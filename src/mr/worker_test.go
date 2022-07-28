package mr

import (
	"fmt"
	"strings"
	"testing"
	"unicode"
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

func TestServant_MapFunc(t *testing.T) {
	mapf := func(document string, value string) (res []KeyValue) {
		m := make(map[string]bool)
		words := strings.FieldsFunc(value, func(x rune) bool { return !unicode.IsLetter(x) })
		for _, w := range words {
			m[w] = true
		}
		for w := range m {
			kv := KeyValue{w, document}
			res = append(res, kv)
		}
		return
	}
	task := &Task{
		MapTask:  true,
		Id:       1,
		MapId:    1,
		FileName: "../main/pg-frankenstein.txt",
		NReduce:  10,
	}
	servant.MapFunc(task, mapf)
}
