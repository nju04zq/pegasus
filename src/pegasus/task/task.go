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
	ReduceTask(Task)
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
	TaskId  string
	Kind    string
	StartTs time.Time
	EndTs   time.Time
	Desc    string
	Output  interface{}
}

func DecodeSpec(tspec *TaskSpec, spec interface{}) error {
	buf, err := json.Marshal(tspec.Spec)
	if err != nil {
		return fmt.Errorf("Fail to marshal tspec, %v", err)
	}
	if err = json.Unmarshal(buf, spec); err != nil {
		return fmt.Errorf("Fail to unmarshal spec, %v", err)
	}
	return nil
}

type TaskGenerator func(tspec *TaskSpec) Task

type Task interface {
	NewTaskletCtx() TaskletCtx
	GetTaskId() string
	GetTaskKind() string
	GetStartTs() time.Time
	GetEndTs() time.Time
	GetDesc() string
	GetNextTasklet() Tasklet
	ReduceTasklet(Tasklet)
	SetError(error)
	GetError() string
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

var taskGens = make(map[string]TaskGenerator)

func RegisterProject() {

}

func GetTaskGenerator(kind string) TaskGenerator {
	gen, ok := taskGens[kind]
	if !ok {
		return nil
	}
	return gen
}

func GenerateTaskReport(tsk Task) *TaskReport {
	return &TaskReport{
		Err:     tsk.GetError(),
		TaskId:  tsk.GetTaskId(),
		Kind:    tsk.GetTaskKind(),
		StartTs: tsk.GetStartTs(),
		EndTs:   tsk.GetEndTs(),
		Desc:    tsk.GetDesc(),
		Output:  tsk.GetOutput(),
	}
}
