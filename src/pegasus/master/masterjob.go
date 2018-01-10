package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"pegasus/log"
	"pegasus/server"
	"pegasus/task"
	"pegasus/util"
	"sync"
	"time"
)

var jobctx = new(JobCtx)

const (
	BUF_TASK_CNT = 10
)

type TaskStats struct {
	total      int
	disptached int
	done       int
}

func (stats *TaskStats) reset() {
	stats.disptached = 0
	stats.done = 0
	stats.total = 0
}

type TaskMeta struct {
	Tid        string
	Kind       string
	StartTs    time.Time
	EndTs      time.Time
	WorkerAddr string
	ErrCnt     int
	ErrMsg     string
	report     *task.TaskReport
}

type JobCtx struct {
	curJob          task.Job
	finish          chan struct{}
	abort           chan struct{}
	todoTasks       chan *task.TaskSpec
	reassignedTasks chan *task.TaskSpec
	// Following fields under mutex protection
	mutex     sync.Mutex
	err       error
	taskMetas map[string]*TaskMeta
	taskStats TaskStats
}

func (ctx *JobCtx) init() {
	jobctx.finish = make(chan struct{})
	jobctx.todoTasks = make(chan *task.TaskSpec, BUF_TASK_CNT)
	jobctx.reassignedTasks = make(chan *task.TaskSpec)
}

func (ctx *JobCtx) reset() {
	log.Info("Reset job ctx")
	jobctx.taskMetas = make(map[string]*TaskMeta)
	ctx.taskStats.reset()
}

func (ctx *JobCtx) assignJob(job task.Job) {
	job.Init()
	ctx.curJob = job
	ctx.taskStats.total = job.CalcTaskCnt()
	log.Info("Total task count %d", jobctx.taskStats.total)
}

func (ctx *JobCtx) setErr(err error) {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	ctx.err = err
}

func (ctx *JobCtx) aborted() bool {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	if ctx.err != nil {
		return true
	} else {
		return false
	}
}

func (ctx *JobCtx) addTaskMeta(tspec *task.TaskSpec) {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	m := &TaskMeta{
		Tid:  tspec.Tid,
		Kind: tspec.Kind,
	}
	if _, ok := ctx.taskMetas[tspec.Tid]; ok {
		log.Error("Task %q meta already set", tspec.Tid)
	}
	ctx.taskMetas[tspec.Tid] = m
}

func (ctx *JobCtx) incDispatched() {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	ctx.taskStats.disptached++
}

func (ctx *JobCtx) incDone() {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	ctx.taskStats.done++
}

func (ctx *JobCtx) allDone() bool {
	res := false
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	res = (ctx.taskStats.done == ctx.taskStats.total)
	return res
}

func taskReportHandler(w http.ResponseWriter, r *http.Request) {
	key, err := getWorkerKeyFromReq(r)
	if err != nil {
		log.Error("Fail to get worker key, %v", err)
		server.FmtResp(w, err, nil)
		return
	}
	body, err := util.HttpReadRequestJsonBody(r)
	if err != nil {
		log.Error("Fail to read request, %v", err)
		server.FmtResp(w, err, nil)
	}
	report := new(task.TaskReport)
	if err = json.Unmarshal(body, report); err != nil {
		err = fmt.Errorf("Fail to unmarshal body for task report, %v, body:\n%s",
			err, string(body))
		log.Error(err.Error())
		server.FmtResp(w, err, nil)
	}
	err = handleTaskReport(key, report)
	server.FmtResp(w, nil, nil)
}

func handleTaskReport(key string, report *task.TaskReport) error {
	if err := wmgr.handleTaskReport(jobctx, key, report); err != nil {
		return err
	}
	jobctx.incDone()
	return nil
}

func taskDispatcher(ctx *JobCtx) {
	log.Info("Task dispatcher working...")
	for {
		var t *task.TaskSpec
		select {
		case t = <-ctx.todoTasks:
			//do nothing
		case t = <-ctx.reassignedTasks:
			//do nothing
		case <-ctx.abort:
			//do nothing
		}
		if ctx.aborted() {
			log.Info("Job ctx was set aborted, exit dispatcher!")
			break
		}
		wmgr.dispatchTask(t)
		ctx.incDispatched()
	}
}

func assignTasks(job task.Job) {
	for {
		tid := generateTid()
		tspec := job.GetNextTask(tid)
		if tspec == nil {
			break
		}
		log.Info("Assign task %q", tspec.Tid)
		jobctx.todoTasks <- tspec
	}
}

func reassignTask(tspec *task.TaskSpec) {
	log.Info("Reassign task %q", tspec.Tid)
	jobctx.reassignedTasks <- tspec
}

func waitForJobDone(ctx *JobCtx) (err error) {
	log.Info("Wait for job %q done", ctx.curJob.GetDesc())
	select {
	case <-ctx.finish:
		break
	}
	return
}

func generateTid() string {
	return fmt.Sprintf("tsk-%d", time.Now().UnixNano())
}

func reduceTasks(ctx *JobCtx) {
	log.Info("Reduce tasks for job")

}

func feedNextJobs(job task.Job) {
	log.Info("Feed output to next level jobs")
	output := job.GetOutput()
	for _, nextJob := range job.GetNextJobs() {
		nextJob.AppendInput(output)
	}
}

func runJob(job task.Job) {
	log.Info("Running job %q", job.GetDesc())
	jobctx.reset()
	jobctx.assignJob(job)
	assignTasks(job)
	waitForJobDone(jobctx)
	reduceTasks(jobctx)
	feedNextJobs(job)
}

func initJobCtx() {
	jobctx.reset()
	go taskDispatcher(jobctx)
}
