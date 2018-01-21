package mergesortjobs

import (
	"math/rand"
	"pegasus/log"
	"pegasus/task"
	"time"
)

const (
	taskSegments = 1
	TaskKind     = "randints"
	RandIntMin   = 1
	RandIntMax   = 100
)

type RandInts struct {
	seed        int64
	tskIndex    int
	tskSegments int
	startTs     time.Time
	endTs       time.Time
}

func (job *RandInts) AppendInput(input interface{}) {
	return
}

func (job *RandInts) Init() {
	job.startTs = time.Now()
	job.seed = job.startTs.UnixNano()
	job.tskIndex = 0
	job.tskSegments = taskSegments
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
	return job.tskSegments
}

func (job *RandInts) GetNextTask(tid string) *task.TaskSpec {
	job.tskIndex++
	if job.tskIndex > job.tskSegments {
		return nil
	}
	spec := &taskSpecRandInts{
		Seed:   job.seed,
		Size:   job.tskIndex * 10,
		MinNum: RandIntMin,
		MaxNum: RandIntMax,
	}
	return &task.TaskSpec{
		Tid:  tid,
		Kind: TaskKind,
		Spec: spec,
	}
}

func (job *RandInts) ReduceTask(report *task.TaskReport) {

}

func (job *RandInts) GetOutput() interface{} {
	return ""
}

func (job *RandInts) GetNextJobs() []task.Job {
	return nil
}

func (job *RandInts) GetTaskGen() task.TaskGenerator {
	return TaskGenRandInts
}

type taskSpecRandInts struct {
	Seed   int64
	Size   int
	MinNum int
	MaxNum int
}

func TaskGenRandInts(tspec *task.TaskSpec, executorCnt int) (task.Task, error) {
	tsk := new(taskRandInts)
	tsk.tid = tspec.Tid
	tsk.kind = tspec.Kind
	tsk.executorCnt = executorCnt
	tsk.startTs = time.Now()
	subspec := new(taskSpecRandInts)
	if err := task.DecodeSpec(tspec, subspec); err != nil {
		return nil, err
	}
	tsk.seed = subspec.Seed
	tsk.total = subspec.Size
	tsk.minNum = subspec.MinNum
	tsk.maxNum = subspec.MaxNum
	tsk.left = tsk.total
	tsk.ints = make([]int, 0)
	log.Info("Generate task %q, seed %d, total %d, left %d",
		tsk.tid, tsk.seed, tsk.total, tsk.left)
	return tsk, nil
}

type taskRandInts struct {
	err         error
	tid         string
	kind        string
	executorCnt int
	desc        string
	startTs     time.Time
	endTs       time.Time
	seed        int64
	total       int
	minNum      int
	maxNum      int
	left        int
	taskletCnt  int
	executorIdx int
	ints        []int
}

type taskletRandIntsCtx struct {
	rand   *rand.Rand
	minNum int
	maxNum int
}

func (ctx *taskletRandIntsCtx) Close() {
}

func (tsk *taskRandInts) NewTaskletCtx() task.TaskletCtx {
	seed := tsk.seed + int64(tsk.executorIdx)
	tsk.executorIdx++
	ctx := new(taskletRandIntsCtx)
	ctx.rand = rand.New(rand.NewSource(seed))
	ctx.minNum = tsk.minNum
	ctx.maxNum = tsk.maxNum
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

func (tsk *taskRandInts) CalcTaskletCnt() int {
	tsk.taskletCnt = tsk.executorCnt * 2
	return tsk.taskletCnt
}

func (tsk *taskRandInts) GetNextTasklet(taskletid string) task.Tasklet {
	log.Info("Randints, get next tasklet, left %d", tsk.left)
	if tsk.left == 0 {
		return nil
	}
	size := (tsk.total + tsk.taskletCnt - 1) / tsk.taskletCnt
	if tsk.left < size {
		size = tsk.left
	}
	tsk.left -= size
	return &taskletRandInts{
		tid:  taskletid,
		size: size,
	}
}

func (tsk *taskRandInts) ReduceTasklet(t task.Tasklet) {
	tasklet := t.(*taskletRandInts)
	tsk.ints = append(tsk.ints, tasklet.ints...)
}

func (tsk *taskRandInts) SetError(err error) {
	tsk.err = err
}

func (tsk *taskRandInts) GetError() error {
	return tsk.err
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

func (t *taskletRandInts) randInt(tctx *taskletRandIntsCtx) int {
	cnt := tctx.maxNum - tctx.minNum + 1
	num := tctx.rand.Int() % cnt
	return num + tctx.minNum
}

func (t *taskletRandInts) Execute(ctx task.TaskletCtx) error {
	tctx := ctx.(*taskletRandIntsCtx)
	t.startTs = time.Now()
	t.ints = make([]int, 0)
	for i := 0; i < t.size; i++ {
		t.ints = append(t.ints, t.randInt(tctx))
	}
	t.endTs = time.Now()
	return nil
}
