package mr

const (
	RETRY_RATE = 0.75 //比例
	HEART_TIME = 20   //ms
	HEART_CNT  = 5
	CHECK_TIME = 5 //ms 检测任务时间
	DIR_PATH   = "../files/"
)

type Task struct {
	MapTask  bool     `json:"map_task"` //是否是Map任务
	Id       int      `json:"id"`       //当前任务的id, 和必须存在, 因为一个reduce_id可能对应多个task_id
	MapId    int      `json:"map_id,omitempty"`
	ReduceId int      `json:"reduce_id,omitempty"`
	FileName string   `json:"file_name"` //该任务读取的文件名
	Files    []string `json:"files"`     //reduce任务需要一堆文件
	NReduce  int      `json:"n_reduce"`  //reduce数目
}
