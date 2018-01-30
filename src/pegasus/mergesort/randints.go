package mergesort

import (
	"math/rand"
	"pegasus/log"
	"pegasus/task"
	"pegasus/util"
	"time"
)

const (
	GEN_SEGMENTS       = 4
	JOB_KIND_RANDINTS  = "Mergesort:randints"
	TASK_KIND_RANDINTS = JOB_KIND_RANDINTS
	MIN_INT            = 1
	MAX_INT            = 100
)

type JobRandInts struct {
	seed     int64
	tskIndex int
	startTs  time.Time
	endTs    time.Time
	output   []int
	nextJobs []*JobMergesort
}

func (job *JobRandInts) AppendInput(input interface{}) {
	return
}

func (job *JobRandInts) Init() error {
	job.startTs = time.Now()
	job.seed = job.startTs.UnixNano()
	job.tskIndex = 0
	job.output = make([]int, 0)
	return nil
}

func (job *JobRandInts) GetStartTs() time.Time {
	return job.startTs
}

func (job *JobRandInts) GetEndTs() time.Time {
	return job.endTs
}

func (job *JobRandInts) GetKind() string {
	return JOB_KIND_RANDINTS
}

func (job *JobRandInts) CalcTaskCnt() int {
	return GEN_SEGMENTS
}

func (job *JobRandInts) GetNextTask(tid string) *task.TaskSpec {
	job.tskIndex++
	if job.tskIndex > GEN_SEGMENTS {
		return nil
	}
	spec := &taskSpecRandInts{
		Seed: job.seed,
		Size: job.tskIndex * 10,
	}
	return &task.TaskSpec{
		Tid:  tid,
		Kind: TASK_KIND_RANDINTS,
		Spec: spec,
	}
}

func (job *JobRandInts) ReduceTasks(reports []*task.TaskReport) error {
	for _, report := range reports {
		a := make([]int, 0)
		if err := util.FitDataInto(report.Output, &a); err != nil {
			return err
		}
		job.output = append(job.output, a...)
	}
	return nil
}

func (job *JobRandInts) GetOutput() interface{} {
	return job.output
}

func (job *JobRandInts) GetNextJobs() []task.Job {
	jobs := make([]task.Job, 0, len(job.nextJobs))
	for _, j := range job.nextJobs {
		jobs = append(jobs, j)
	}
	return jobs
}

func (job *JobRandInts) GetTaskGen() task.TaskGenerator {
	return TaskGenRandInts
}

type taskSpecRandInts struct {
	Seed int64
	Size int
}

func TaskGenRandInts(tspec *task.TaskSpec) (task.Task, error) {
	tsk := new(taskRandInts)
	tsk.tid = tspec.Tid
	tsk.kind = tspec.Kind
	spec := new(taskSpecRandInts)
	task.DecodeSpec(tspec, spec)
	tsk.seed = spec.Seed
	tsk.total = spec.Size
	tsk.left = spec.Size
	tsk.ints = make([]int, 0)
	return tsk, nil
}

type taskRandInts struct {
	err         error
	tid         string
	kind        string
	desc        string
	startTs     time.Time
	endTs       time.Time
	seed        int64
	total       int
	left        int
	executorCnt int
	taskletCnt  int
	ints        []int
}

type taskletRandIntsCtx struct {
	rand *rand.Rand
}

func (ctx *taskletRandIntsCtx) Close() {
}

func (tsk *taskRandInts) Init(executorCnt int) error {
	tsk.startTs = time.Now()
	tsk.executorCnt = executorCnt
	tsk.taskletCnt = (tsk.total + tsk.executorCnt - 1) / tsk.executorCnt
	return nil
}

func (tsk *taskRandInts) NewTaskletCtx() task.TaskletCtx {
	seed := tsk.seed + time.Now().UnixNano()
	log.Info("Generate seed %d for tasklet ctx", seed)
	ctx := new(taskletRandIntsCtx)
	ctx.rand = rand.New(rand.NewSource(seed))
	return ctx
}

func (tsk *taskRandInts) GetTaskId() string {
	return tsk.tid
}

func (tsk *taskRandInts) GetKind() string {
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

func (tsk *taskRandInts) GetTaskletCnt() int {
	return tsk.taskletCnt
}

func (tsk *taskRandInts) GetNextTasklet(taskletid string) task.Tasklet {
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

func (tsk *taskRandInts) ReduceTasklets(tasklets []task.Tasklet) {
	for _, t := range tasklets {
		tasklet := t.(*taskletRandInts)
		log.Info("From %q, ints %v", tasklet.tid, tasklet.ints)
		tsk.ints = append(tsk.ints, tasklet.ints...)
	}
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

func (t *taskletRandInts) randInt(rand *rand.Rand) int {
	d := MAX_INT - MIN_INT + 1
	return rand.Int()%d + MIN_INT
}

func (t *taskletRandInts) Execute(ctx task.TaskletCtx) error {
	tctx := ctx.(*taskletRandIntsCtx)
	t.startTs = time.Now()
	t.ints = make([]int, 0)
	for i := 0; i < t.size; i++ {
		t.ints = append(t.ints, t.randInt(tctx.rand))
	}
	t.endTs = time.Now()
	time.Sleep(500 * time.Millisecond)
	return nil
}
