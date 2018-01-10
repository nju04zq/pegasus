package jobs

import (
	"fmt"
	"math/rand"
	"pegasus/task"
	"time"
)

const (
	genSegments = 8
)

type RandInts struct {
	seed     int64
	tskIndex int
	startTs  time.Time
	endTs    time.Time
}

func (job *RandInts) AppendInput(input interface{}) {
	return
}

func (job *RandInts) Init() {
	job.startTs = time.Now()
	job.seed = job.startTs.UnixNano()
	job.tskIndex = 0
	return
}

func (job *RandInts) GetStartTs() time.Time {
	return job.startTs
}

func (job *RandInts) GetEndTs() time.Time {
	return job.endTs
}

func (job *RandInts) GetDesc() string {
	return "Generate random int sequence"
}

func (job *RandInts) CalcTaskCnt() int {
	return genSegments
}

func (job *RandInts) GetNextTask(tid string) *task.TaskSpec {
	job.tskIndex++
	spec := &taskSpecRandInts{
		seed: job.seed,
		size: job.tskIndex * 10,
	}
	return &task.TaskSpec{
		Tid:  tid,
		Kind: "",
		Spec: spec,
	}
}

func (job *RandInts) ReduceTask(tsk task.Task) {

}

func (job *RandInts) GetOuptut() interface{} {
	return ""
}

func (job *RandInts) GetNextJobs() []task.Job {
	return nil
}

func (job *RandInts) GetTaskGen() task.TaskGenerator {
	return taskGenRandInts
}

type taskSpecRandInts struct {
	seed int64
	size int
}

func taskGenRandInts(tspec *task.TaskSpec) task.Task {
	tsk := new(taskRandInts)
	tsk.tid = tspec.Tid
	tsk.kind = tspec.Kind
	tsk.startTs = time.Now()
	// TODO handle error
	spec := new(taskSpecRandInts)
	task.DecodeSpec(tspec, spec)
	tsk.seed = spec.seed
	tsk.total = spec.size
	tsk.left = spec.size
	tsk.ints = make([]int, 0)
	return tsk
}

type taskRandInts struct {
	tid        string
	kind       string
	desc       string
	startTs    time.Time
	endTs      time.Time
	seed       int64
	total      int
	left       int
	tskletIdx  int
	taskletCnt int
	ints       []int
}

type taskletRandIntsCtx struct {
	rand *rand.Rand
}

func (ctx *taskletRandIntsCtx) Close() {
}

func (tsk *taskRandInts) NewTaskletCtx() task.TaskletCtx {
	seed := tsk.seed + int64(tsk.taskletCnt)
	tsk.taskletCnt++
	ctx := new(taskletRandIntsCtx)
	ctx.rand = rand.New(rand.NewSource(seed))
	return ctx
}

func (tsk *taskRandInts) GetTaskId() string {
	return tsk.tid
}

func (tsk *taskRandInts) GetTaskKind() string {
	return tsk.kind
}

func (tsk *taskRandInts) GetStartTs() time.Time {
	return tsk.startTs
}

func (tsk *taskRandInts) GetEndTs() time.Time {
	return tsk.endTs
}

func (tsk *taskRandInts) GetDesc() string {
	return tsk.desc
}

func (tsk *taskRandInts) GetNextTasklet() task.Tasklet {
	if tsk.left == 0 {
		return nil
	}
	size := tsk.total / tsk.taskletCnt
	if tsk.left < size {
		size = tsk.left
	}
	return &taskletRandInts{
		tid:  fmt.Sprintf("%s-%d", tsk.tid, tsk.tskletIdx),
		size: size,
	}
}

func (tsk *taskRandInts) ReduceTasklet(t task.Tasklet) {
	tasklet := t.(*taskletRandInts)
	tsk.ints = append(tsk.ints, tasklet.ints...)
}

func (tsk *taskRandInts) GetOutput() interface{} {
	return tsk.ints
}

type taskletRandInts struct {
	tid     string
	startTs time.Time
	endTs   time.Time
	size    int
	ints    []int
}

func (t *taskletRandInts) GetTaskletId() string {
	return t.tid
}

func (t *taskletRandInts) GetStartTs() time.Time {
	return t.startTs
}

func (t *taskletRandInts) GetEndTs() time.Time {
	return t.endTs
}

func (t *taskletRandInts) Execute(ctx task.TaskletCtx) {
	tctx := ctx.(*taskletRandIntsCtx)
	t.startTs = time.Now()
	t.ints = make([]int, 0)
	for i := 0; i < t.size; i++ {
		t.ints = append(t.ints, tctx.rand.Int())
	}
	t.endTs = time.Now()
}
