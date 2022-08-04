package raft

import (
	"fmt"
	"log"
	"time"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func MyPrintf(me int, term int, index int, format string, a ...interface{}) string {
	str := fmt.Sprintf("%v at %v-term %v-index: ", me, term, index)
	ans := fmt.Sprintf(str+format, a...)
	DPrintf(str+format, a...)
	return ans
}

func NowMillSecond() int64 {
	return time.Now().UnixNano() / 1000000
}

func GetMillSecondInt64(num int64) int64 {
	return int64(GetMillSecond(num))
}

func GetMillSecond(num int64) time.Duration {
	return time.Duration(num) * time.Millisecond
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
