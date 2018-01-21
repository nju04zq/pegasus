package task

import (
	"encoding/json"
	"fmt"
	"time"
)

type Project interface {
	GetJobs() []Job
}

type Job interface {
	AppendInput(input interface{})
	Init()
	GetStartTs() time.Time
	GetEndTs() time.Time
	GetDesc() string
	CalcTaskCnt() int
	GetNextTask(tid string) *TaskSpec
	ReduceTask(*TaskReport)
	GetOutput() interface{}
	GetNextJobs() []Job
	GetTaskGen() TaskGenerator
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

type TaskReport struct {
	Err     string
	Tid     string
	Kind    string
	StartTs time.Time
	EndTs   time.Time
	Desc    string
	Output  interface{}
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

type TaskGenerator func(tspec *TaskSpec, executorCnt int) (Task, error)

type Task interface {
	NewTaskletCtx() TaskletCtx
	GetTaskId() string
	GetTaskKind() string
	GetStartTs() time.Time
	GetEndTs() time.Time
	GetDesc() string
	CalcTaskletCnt() int
	GetNextTasklet(string) Tasklet
	ReduceTasklet(Tasklet)
	SetError(error)
	GetError() error
	GetOutput() interface{}
}

type Tasklet interface {
	GetTaskletId() string
	GetStartTs() time.Time
	GetEndTs() time.Time
	Execute(TaskletCtx) error
}

type TaskletCtx interface {
	Close()
}

func GenerateTaskReport(tsk Task) *TaskReport {
	errMsg := ""
	if err := tsk.GetError(); err != nil {
		errMsg = err.Error()
	}
	return &TaskReport{
		Err:     errMsg,
		Tid:     tsk.GetTaskId(),
		Kind:    tsk.GetTaskKind(),
		StartTs: tsk.GetStartTs(),
		EndTs:   tsk.GetEndTs(),
		Desc:    tsk.GetDesc(),
		Output:  tsk.GetOutput(),
	}
}
