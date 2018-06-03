package task

import (
	"encoding/json"
	"fmt"
	"time"
)

type Project interface {
	Init(config string) error
	GetEnv() interface{}
	GetName() string
	GetJobs() []Job
	Finish() error
}

type Job interface {
	AppendInput(input interface{})
	Init(env interface{}) error
	GetKind() string
	CalcTaskCnt() int
	GetNextTask(tid string) *TaskSpec
	ReduceTasks([]*TaskReport) error
	GetOutput() interface{}
	GetNextJobs() []Job
	GetTaskGen() TaskGenerator
}

type JobStatus struct {
	Kind       string
	Total      int
	Dispatched int
	Done       int
	Detail     []*TaskStatus
}

const (
	TaskStatusOk = iota
	TaskStatusExpired
)

type TaskSpec struct {
	Tid  string
	Kind string
	Spec interface{}
}

func DecodeSpec(tspec *TaskSpec, subspec interface{}) error {
	buf, err := json.Marshal(tspec.Spec)
	if err != nil {
		return fmt.Errorf("Fail to marshal tspec, %v", err)
	}
	if err = json.Unmarshal(buf, subspec); err != nil {
		return fmt.Errorf("Fail to unmarshal spec, %v", err)
	}
	return nil
}

type TaskGenerator func(tspec *TaskSpec) (Task, error)

type Task interface {
	Init(int) error
	NewTaskletCtx() TaskletCtx
	GetTaskId() string
	GetKind() string
	GetDesc() string
	GetTaskletCnt() int
	GetNextTasklet(string) Tasklet
	ReduceTasklets([]Tasklet)
	SetError(error)
	GetError() error
	GetOutput() interface{}
}

type Tasklet interface {
	GetTaskletId() string
	Execute(TaskletCtx) error
}

type TaskletCtx interface {
	Close()
}

type TaskReport struct {
	Err     string
	Tid     string
	Kind    string
	StartTs time.Time
	EndTs   time.Time
	Status  *TaskStatus
	Output  interface{}
}

type TaskStatus struct {
	Tid      string
	Desc     string
	StartTs  time.Time
	Finished bool
	Total    int
	Done     int
}
