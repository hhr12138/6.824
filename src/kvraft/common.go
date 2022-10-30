package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

const (
	CLIENT_SLEEP_TIME            = 10
	WAIT_CHANNEL_RESP_SLEEP_TIME = 50 //ms
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	RequestId string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
	Code
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RequestId string
}

type RemoveArgs struct {
	RequestId       string
	RemoveRequestId string
}
type RemoveReply struct {
	Err string
	Code
}

type Code int

const (
	NOT_LEADER     Code = 1
	REPEAT_REQUEST Code = 2
	SUCCESS        Code = 3
)

type GetReply struct {
	Err   Err
	Code  Code
	Value string
}
