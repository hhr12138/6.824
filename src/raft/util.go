package raft

import (
	"fmt"
	"log"
	"time"
)

// Debugging
const IsNotDebug = 1
const nowLogLevel = SLock
const PrintLock = 1

type LogLevel int

const (
	Debug   LogLevel = 0
	Info    LogLevel = 1
	Warn    LogLevel = 2
	Error   LogLevel = 3
	Critcal LogLevel = 4
	Lock    LogLevel = 5
	SLock   LogLevel = 6
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if IsNotDebug > 0 {
		log.Printf(format, a...)
	}
	return
}

func MyPrintf(level LogLevel, me int, term int, index int, format string, a ...interface{}) string {
	if level < nowLogLevel {
		return ""
	}
	if level == Lock && PrintLock <= 0 {
		return ""
	}
	str := fmt.Sprintf("level=%v, %v at %v-term %v-index: ", level, me, term, index)
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
