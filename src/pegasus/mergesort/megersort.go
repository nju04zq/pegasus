package mergesort

import (
	"pegasus/task"
	"pegasus/util"
	"sort"
	"time"
)

const (
	SPLIT_SEGMENTS      = 8
	JOB_KIND_MERGESORT  = "Mergesort:mergesort"
	TASK_KIND_MERGESORT = JOB_KIND_MERGESORT
)

type JobMergesort struct {
	input     []int
	startTs   time.Time
	endTs     time.Time
	total     int
	nextStart int
	tskSize   int
	output    []int
	nextJobs  []*JobDumpres
}

func (job *JobMergesort) AppendInput(input interface{}) {
	a := input.([]int)
	if job.input == nil {
		job.input = make([]int, 0)
	}
	job.input = append(job.input, a...)
	return
}

func (job *JobMergesort) Init() error {
	job.total = len(job.input)
	job.startTs = time.Now()
	job.output = make([]int, 0)
	job.tskSize = (job.total + SPLIT_SEGMENTS - 1) / SPLIT_SEGMENTS
	return nil
}

func (job *JobMergesort) GetStartTs() time.Time {
	return job.startTs
}

func (job *JobMergesort) GetEndTs() time.Time {
	return job.endTs
}

func (job *JobMergesort) GetKind() string {
	return JOB_KIND_MERGESORT
}

func (job *JobMergesort) CalcTaskCnt() int {
	return SPLIT_SEGMENTS
}

func (job *JobMergesort) GetNextTask(tid string) *task.TaskSpec {
	if job.nextStart >= job.total {
		return nil
	}
	end := job.nextStart + job.tskSize
	if end > job.total {
		end = job.total
	}
	spec := &taskSpecMergesort{
		seq: job.input[job.nextStart:end],
	}
	return &task.TaskSpec{
		Tid:  tid,
		Kind: TASK_KIND_MERGESORT,
		Spec: spec,
	}
}

func (job *JobMergesort) ReduceTasks(reports []*task.TaskReport) error {
	all := make([]int, 0)
	for _, report := range reports {
		a := make([]int, 0)
		if err := util.FitDataInto(report.Output, &a); err != nil {
			return err
		}
		all = append(all, a...)
	}
	sort.Ints(all)
	job.output = all
	return nil
}

func (job *JobMergesort) GetOutput() interface{} {
	return job.output
}

func (job *JobMergesort) GetNextJobs() []task.Job {
	return nil
}

func (job *JobMergesort) GetTaskGen() task.TaskGenerator {
	return TaskGenMergesort
}

type taskSpecMergesort struct {
	seq []int
}

func TaskGenMergesort(tspec *task.TaskSpec) (task.Task, error) {
	tsk := new(taskMergesort)
	tsk.tid = tspec.Tid
	tsk.kind = tspec.Kind
	spec := new(taskSpecMergesort)
	task.DecodeSpec(tspec, spec)
	tsk.seq = spec.seq
	return tsk, nil
}

type taskMergesort struct {
	err        error
	tid        string
	kind       string
	desc       string
	startTs    time.Time
	endTs      time.Time
	taskletCnt int
	left       int
	seq        []int
	output     []int
}

type taskletMergsortCtx struct {
}

func (ctx *taskletMergsortCtx) Close() {
}

func (tsk *taskMergesort) Init(executorCnt int) error {
	tsk.startTs = time.Now()
	tsk.taskletCnt = 1
	tsk.left = tsk.taskletCnt
	return nil
}

func (tsk *taskMergesort) NewTaskletCtx() task.TaskletCtx {
	ctx := new(taskletMergsortCtx)
	return ctx
}

func (tsk *taskMergesort) GetTaskId() string {
	return tsk.tid
}

func (tsk *taskMergesort) GetKind() string {
	return tsk.kind
}

func (tsk *taskMergesort) GetStartTs() time.Time {
	return tsk.startTs
}

func (tsk *taskMergesort) GetEndTs() time.Time {
	return tsk.endTs
}

func (tsk *taskMergesort) GetDesc() string {
	return tsk.desc
}

func (tsk *taskMergesort) GetTaskletCnt() int {
	return tsk.taskletCnt
}

func (tsk *taskMergesort) GetNextTasklet(taskletid string) task.Tasklet {
	if tsk.left == 0 {
		return nil
	}
	return &taskletMergesort{
		tid: taskletid,
		seq: tsk.seq,
	}
}

func (tsk *taskMergesort) ReduceTasklets(tasklets []task.Tasklet) {
	for _, t := range tasklets {
		tasklet := t.(*taskletMergesort)
		tsk.output = tasklet.seq
	}
}

func (tsk *taskMergesort) SetError(err error) {
	tsk.err = err
}

func (tsk *taskMergesort) GetError() error {
	return tsk.err
}

func (tsk *taskMergesort) GetOutput() interface{} {
	return tsk.output
}

type taskletMergesort struct {
	tid     string
	startTs time.Time
	endTs   time.Time
	seq     []int
}

func (t *taskletMergesort) GetTaskletId() string {
	return t.tid
}

func (t *taskletMergesort) GetStartTs() time.Time {
	return t.startTs
}

func (t *taskletMergesort) GetEndTs() time.Time {
	return t.endTs
}

func (t *taskletMergesort) Execute(ctx task.TaskletCtx) error {
	t.startTs = time.Now()
	sort.Ints(t.seq)
	t.endTs = time.Now()
	time.Sleep(500 * time.Millisecond)
	return nil
}
