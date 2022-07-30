package mr

import (
	"fmt"
	"io/ioutil"
	"sort"
	"strings"
	"testing"
	"unicode"
)

var servant *Servant
var reducef func(string, []string) string
var mapf func(string, string) []KeyValue

func init() {
	servant = MakeServant()
	reducef = func(key string, values []string) string {
		sort.Strings(values)
		return fmt.Sprintf("%d %s", len(values), strings.Join(values, ","))
	}
	mapf = func(document string, value string) (res []KeyValue) {
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
	task := &Task{
		MapTask:  true,
		Id:       1,
		MapId:    1,
		FileName: "../main/pg-frankenstein.txt",
		NReduce:  10,
	}
	servant.MapFunc(task, mapf)
}

func TestServant_ReduceFunc(t *testing.T) {
	m := MakeMaster(nil, 10)
	files, _ := ioutil.ReadDir(DIR_PATH)
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		fileName := file.Name()
		task := &Task{
			MapId:    1,
			ReduceId: 1,
			Id:       m.GetTaskId(1, 1),
			MapTask:  false,
			FileName: fileName,
		}
		servant.ReduceFunc(task, reducef)
	}
}

func TestWorker(t *testing.T) {
	Worker(mapf, reducef)
}
