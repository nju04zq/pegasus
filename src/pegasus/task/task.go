package task

import (
	"encoding/json"
	"fmt"
	"time"
)

type Project interface {
	Init() error
	GetName() string
	GetJobs() []Job
	GetStartTs() time.Time
	GetEndTs() time.Time
	SetErr(error)
	GetErr() error
	Finish() error
}

type Job interface {
	AppendInput(input interface{})
	Init() error
	GetStartTs() time.Time
	GetEndTs() time.Time
	GetKind() string
	CalcTaskCnt() int
	GetNextTask(tid string) *TaskSpec
	ReduceTasks([]*TaskReport) error
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
	GetStartTs() time.Time
	GetEndTs() time.Time
	GetTaskletCnt() int
	GetNextTasklet(string) Tasklet
	ReduceTasklets([]Tasklet)
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

type TaskReport struct {
	Err     string
	Tid     string
	Kind    string
	StartTs time.Time
	EndTs   time.Time
	Output  interface{}
}

func GenerateTaskReport(tsk Task) *TaskReport {
	errMsg := ""
	if err := tsk.GetError(); err != nil {
		errMsg = err.Error()
	}
	return &TaskReport{
		Err:     errMsg,
		Tid:     tsk.GetTaskId(),
		Kind:    tsk.GetKind(),
		StartTs: tsk.GetStartTs(),
		EndTs:   tsk.GetEndTs(),
		Output:  tsk.GetOutput(),
	}
}
