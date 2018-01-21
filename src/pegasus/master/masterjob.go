package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"pegasus/log"
	"pegasus/mergesort/mergesortjobs"
	"pegasus/server"
	"pegasus/task"
	"pegasus/util"
	"sync"
	"time"
)

var jobctx = new(JobCtx)

const (
	BUF_TASK_CNT = 10
	TASK_MAX_ERR = 5
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
	tspec      *task.TaskSpec
	report     *task.TaskReport
}

type JobCtx struct {
	curJob          task.Job
	finish          chan struct{}
	todoTasks       chan *task.TaskSpec
	reassignedTasks chan *task.TaskSpec
	// Following fields under mutex protection
	mutex     sync.Mutex
	err       error
	taskMetas map[string]*TaskMeta
	taskStats TaskStats
}

func (ctx *JobCtx) reset() {
	log.Info("Reset job ctx")
	jobctx.finish = make(chan struct{})
	jobctx.todoTasks = make(chan *task.TaskSpec, BUF_TASK_CNT)
	jobctx.reassignedTasks = make(chan *task.TaskSpec)
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
	ctx.finish <- struct{}{}
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
		Tid:   tspec.Tid,
		Kind:  tspec.Kind,
		tspec: tspec,
	}
	if _, ok := ctx.taskMetas[tspec.Tid]; ok {
		log.Error("Task %q meta already set", tspec.Tid)
	}
	ctx.taskMetas[tspec.Tid] = m
}

func (ctx *JobCtx) getTaskMeta(tid string) *TaskMeta {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	if m, ok := ctx.taskMetas[tid]; !ok {
		log.Error("Task %q meta not found", tid)
		return nil
	} else {
		return m
	}
}

func (ctx *JobCtx) updateTaskMetaForWorker(tid, workerAddr string) {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	m, ok := ctx.taskMetas[tid]
	if !ok {
		log.Error("Task %q meta info not found", tid)
		return
	}
	m.WorkerAddr = workerAddr
}

func (ctx *JobCtx) updateTaskMetaForReport(report *task.TaskReport) {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	m, ok := ctx.taskMetas[report.Tid]
	if !ok {
		log.Error("Task %q meta info not found", report.Tid)
		return
	}
	if report.Err == "" {
		m.StartTs = report.StartTs
		m.EndTs = report.EndTs
		m.report = report
	} else {
		m.ErrCnt++
		m.ErrMsg = report.Err
	}
}

func (ctx *JobCtx) getTaskErr(tid string) (int, string) {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	m, ok := ctx.taskMetas[tid]
	if !ok {
		log.Error("Task %q meta info not found", tid)
		return 0, ""
	}
	return m.ErrCnt, m.ErrMsg
}

func (ctx *JobCtx) incDispatched() {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	ctx.taskStats.disptached++
}

func (ctx *JobCtx) incDone() {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	stats := ctx.taskStats
	stats.done++
	if stats.done == stats.total {
		ctx.finish <- struct{}{}
	}
}

func taskReportHandler(w http.ResponseWriter, r *http.Request) {
	// TODO handle preprocess in a more graceful manner
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
		return
	}
	log.Info("Get task report from %q:\n%s", key, body)
	report := new(task.TaskReport)
	if err = json.Unmarshal(body, report); err != nil {
		err = fmt.Errorf("Fail to unmarshal body for task report, %v, body:\n%s",
			err, string(body))
		log.Error(err.Error())
		server.FmtResp(w, err, nil)
		return
	}
	err = handleTaskReport(key, report)
	server.FmtResp(w, nil, nil)
}

func handleTaskReport(key string, report *task.TaskReport) error {
	log.Info("Handle task report from %q, task %q", key, report.Tid)
	if err := wmgr.handleTaskReport(jobctx, key, report); err != nil {
		log.Error("Fail handle task report, %v", err)
		return err
	} else {
		jobctx.incDone()
	}
	if report.Err == "" {
		jobctx.updateTaskMetaForReport(report)
	} else {
		if m := jobctx.getTaskMeta(report.Tid); m != nil {
			reassignTask(m.tspec)
		}
	}
	return nil
}

func taskDispatcher(ctx *JobCtx) {
	log.Info("Task dispatcher working...")
	var t *task.TaskSpec
	var ok bool
	todoTasks, reassignTasks := ctx.todoTasks, ctx.reassignedTasks
	for {
		select {
		case t, ok = <-todoTasks:
			if !ok {
				todoTasks = nil
				log.Info("Todo task channel closed, exit dispatcher.")
			}
		case t, ok = <-ctx.reassignedTasks:
			if !ok {
				reassignTasks = nil
				log.Info("Reassign task channel closed, exit dispatcher.")
			}
		}
		if todoTasks == nil || reassignTasks == nil {
			break
		}
		if ctx.aborted() {
			log.Info("Job ctx was set aborted, exit dispatcher!")
			break
		}
		workerAddr, err := wmgr.dispatchTask(t)
		if err != nil {
			jobctx.setErr(err)
			log.Error("Fail to dispatch task %q, exit dispatcher, %v", t.Tid, err)
			break
		}
		ctx.incDispatched()
		ctx.updateTaskMetaForWorker(t.Tid, workerAddr)
	}
	log.Info("Exit dispatcher")
}

func assignTasks(ctx *JobCtx, job task.Job) error {
	for {
		tid := generateTid()
		tspec := job.GetNextTask(tid)
		if tspec == nil {
			break
		}
		log.Info("Assign task %q", tspec.Tid)
		jobctx.addTaskMeta(tspec)
		select {
		case jobctx.todoTasks <- tspec:
			// do nothing
		case <-ctx.finish:
			return fmt.Errorf("Abort assign task, %v", ctx.err)
		}
	}
	return nil
}

func reassignTask(tspec *task.TaskSpec) {
	log.Info("Reassign task %q", tspec.Tid)
	errCnt, errMsg := jobctx.getTaskErr(tspec.Tid)
	if errCnt > TASK_MAX_ERR {
		err := fmt.Errorf("Task %q failed %d times, last error: %s",
			tspec.Tid, errCnt, errMsg)
		jobctx.setErr(err)
	} else {
		jobctx.reassignedTasks <- tspec
		jobctx.updateTaskMetaForWorker(tspec.Tid, "")
	}
}

func waitForJobDone(ctx *JobCtx) error {
	log.Info("Wait for job %q done", ctx.curJob.GetDesc())
	select {
	case <-ctx.finish:
		break
	}
	close(ctx.todoTasks)
	close(ctx.reassignedTasks)
	return ctx.err
}

func generateTid() string {
	return fmt.Sprintf("tsk-%d", time.Now().UnixNano())
}

func reduceTasks(ctx *JobCtx) {
	log.Info("Reduce tasks for job")
	for _, m := range ctx.taskMetas {
		ctx.curJob.ReduceTask(m.report)
	}
}

func feedNextJobs(job task.Job) {
	log.Info("Feed output to next level jobs")
	output := job.GetOutput()
	for _, nextJob := range job.GetNextJobs() {
		nextJob.AppendInput(output)
	}
}

func runJob(job task.Job) error {
	log.Info("Running job %q", job.GetDesc())
	jobctx.reset()
	go taskDispatcher(jobctx)
	jobctx.assignJob(job)
	if err := assignTasks(jobctx, job); err != nil {
		return err
	}
	if err := waitForJobDone(jobctx); err != nil {
		return err
	}
	reduceTasks(jobctx)
	feedNextJobs(job)
	log.Info("Run job %q done", job.GetDesc())
	return nil
}

func testRunHandler(w http.ResponseWriter, r *http.Request) {
	err := runJob(new(mergesortjobs.RandInts))
	server.FmtResp(w, err, nil)
}
