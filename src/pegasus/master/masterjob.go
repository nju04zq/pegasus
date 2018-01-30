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
	TASK_MAX_ERR = 5
)

type TaskStats struct {
	total      int
	disptached int
	done       int
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
	taskStats *TaskStats
}

func (ctx *JobCtx) reset() {
	log.Info("Reset job ctx")
	jobctx.err = nil
	jobctx.finish = make(chan struct{})
	jobctx.todoTasks = make(chan *task.TaskSpec, BUF_TASK_CNT)
	jobctx.reassignedTasks = make(chan *task.TaskSpec, BUF_TASK_CNT)
	jobctx.taskMetas = make(map[string]*TaskMeta)
	ctx.taskStats = new(TaskStats)
}

func (ctx *JobCtx) assignJob(job task.Job) error {
	log.Info("Assign and init job %q", job.GetKind())
	if err := job.Init(); err != nil {
		err = fmt.Errorf("Fail to init job %q, %v", job.GetKind(), err)
		log.Error(err.Error())
		return err
	}
	ctx.curJob = job
	ctx.taskStats.total = job.CalcTaskCnt()
	log.Info("Total task count %d", jobctx.taskStats.total)
	return nil
}

func (ctx *JobCtx) setErr(err error) {
	log.Info("Set err %v to job ctx", err)
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
	log.Info("Get task report from %q", key)
	log.Debug("Task report from %q:\n%s", key, body)
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
	idx := 0
	for {
		tid := generateTid(idx)
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
		idx++
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
	log.Info("Wait for job %q done", ctx.curJob.GetKind())
	select {
	case <-ctx.finish:
		break
	}
	close(ctx.todoTasks)
	close(ctx.reassignedTasks)
	return ctx.err
}

func generateTid(idx int) string {
	return fmt.Sprintf("tsk-%d-%d", time.Now().UnixNano(), idx)
}

func reduceTasks(ctx *JobCtx) error {
	log.Info("Reduce tasks for job")
	reports := make([]*task.TaskReport, 0)
	for _, m := range ctx.taskMetas {
		reports = append(reports, m.report)
	}
	err := ctx.curJob.ReduceTasks(reports)
	if err != nil {
		log.Error("Fail to reduce task, %v", err)
		return err
	}
	return nil
}

func feedNextJobs(job task.Job) {
	log.Info("Feed output to next level jobs")
	output := job.GetOutput()
	log.Debug("Job %q output:\n%v", job.GetKind(), output)
	for _, nextJob := range job.GetNextJobs() {
		log.Info("Append output to job %q", nextJob.GetKind())
		nextJob.AppendInput(output)
	}
}

func splitJobAndRun(job task.Job) error {
	go taskDispatcher(jobctx)
	if err := jobctx.assignJob(job); err != nil {
		return err
	}
	if err := assignTasks(jobctx, job); err != nil {
		return err
	}
	if err := waitForJobDone(jobctx); err != nil {
		return err
	}
	if err := reduceTasks(jobctx); err != nil {
		return err
	}
	return nil
}

func runJob(job task.Job) error {
	log.Info("Running job %q", job.GetKind())
	jobctx.reset()
	if err := jobctx.assignJob(job); err != nil {
		return err
	}
	if jobctx.taskStats.total > 0 {
		if err := splitJobAndRun(job); err != nil {
			return err
		}
	}
	feedNextJobs(job)
	log.Info("Run job %q done", job.GetKind())
	return nil
}
