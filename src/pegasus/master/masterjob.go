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

type TaskMeta struct {
	Tid         string
	Kind        string
	Desc        string
	StartTs     time.Time
	EndTs       time.Time
	WorkerLabel string
	ErrCnt      int
	ErrMsg      string
	Total       int
	Done        int
	Dispatched  bool
	Updated     bool
	Finished    bool
	tspec       *task.TaskSpec
	report      *task.TaskReport
}

func (tmeta *TaskMeta) snapshot() *TaskMeta {
	return &TaskMeta{
		Tid:         tmeta.Tid,
		Kind:        tmeta.Kind,
		Desc:        tmeta.Desc,
		StartTs:     tmeta.StartTs,
		EndTs:       tmeta.EndTs,
		WorkerLabel: tmeta.WorkerLabel,
		ErrCnt:      tmeta.ErrCnt,
		ErrMsg:      tmeta.ErrMsg,
		Total:       tmeta.Total,
		Done:        tmeta.Done,
		Dispatched:  tmeta.Dispatched,
		Updated:     tmeta.Updated,
		Finished:    tmeta.Finished,
	}
}

type JobMeta struct {
	Kind       string
	StartTs    time.Time
	EndTs      time.Time
	ErrMsg     string
	err        error
	Finished   bool
	Total      int
	Dispatched int
	Done       int
	TaskMetas  []*TaskMeta
	taskMetas  map[string]*TaskMeta
}

func (m *JobMeta) Init() *JobMeta {
	m.TaskMetas = make([]*TaskMeta, 0)
	m.taskMetas = make(map[string]*TaskMeta)
	return m
}

func (m *JobMeta) setJob(job task.Job) {
	m.Kind = job.GetKind()
	m.Total = job.CalcTaskCnt()
	log.Info("Total task count %d", m.Total)
}

func (m *JobMeta) setErr(err error) {
	m.err = err
	m.ErrMsg = err.Error()
}

func (m *JobMeta) getErr() error {
	return m.err
}

func (m *JobMeta) incDispatched() {
	m.Dispatched++
}

func (m *JobMeta) incDone() {
	m.Done++
}

func (m *JobMeta) allDone() bool {
	return m.Total == m.Done
}

func (m *JobMeta) addTaskMeta(tspec *task.TaskSpec) {
	tmeta := &TaskMeta{
		Tid:   tspec.Tid,
		Kind:  tspec.Kind,
		tspec: tspec,
	}
	if _, ok := m.taskMetas[tspec.Tid]; ok {
		log.Error("Task %q meta already set", tspec.Tid)
	}
	m.taskMetas[tspec.Tid] = tmeta
	m.TaskMetas = append(m.TaskMetas, tmeta)
}

func (m *JobMeta) getTaskMeta(tid string) *TaskMeta {
	if tmeta, ok := m.taskMetas[tid]; !ok {
		log.Error("Task %q meta not found", tid)
		return nil
	} else {
		return tmeta
	}
}

func (m *JobMeta) addTaskReport(report *task.TaskReport) {
	m.updateTaskStatus(report.Status)
	tmeta := m.getTaskMeta(report.Tid)
	if tmeta == nil {
		log.Error("Task %q meta info not found", report.Tid)
		return
	}
	tmeta.StartTs = report.StartTs
	tmeta.EndTs = report.EndTs
	if report.Err == "" {
		tmeta.report = report
	} else {
		tmeta.ErrCnt++
		tmeta.ErrMsg = report.Err
	}
}

func (m *JobMeta) updateTaskStatus(status *task.TaskStatus) {
	tmeta := m.getTaskMeta(status.Tid)
	if tmeta == nil {
		log.Error("Task %q meta info not found", status.Tid)
		return
	}
	tmeta.Desc = status.Desc
	tmeta.StartTs = status.StartTs
	tmeta.Finished = status.Finished
	tmeta.Total = status.Total
	tmeta.Done = status.Done
	tmeta.Updated = true
}

func (m *JobMeta) snapshot() *JobMeta {
	tmetas := make([]*TaskMeta, 0, len(m.TaskMetas))
	for _, tmeta := range m.TaskMetas {
		if tmeta.Dispatched {
			tmetas = append(tmetas, tmeta.snapshot())
		}
	}
	return &JobMeta{
		Kind:       m.Kind,
		StartTs:    m.StartTs,
		EndTs:      m.EndTs,
		ErrMsg:     m.ErrMsg,
		Finished:   m.Finished,
		Total:      m.Total,
		Dispatched: m.Dispatched,
		Done:       m.Done,
		TaskMetas:  tmetas,
	}
}

type JobCtx struct {
	curJob          task.Job
	shouldFinish    chan struct{}
	todoTasks       chan *task.TaskSpec
	reassignedTasks chan *task.TaskSpec
	// Following fields under mutex protection
	mutex   sync.Mutex
	jobMeta *JobMeta
}

func (ctx *JobCtx) start() {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	log.Info("Reset job ctx")
	ctx.shouldFinish = make(chan struct{})
	ctx.todoTasks = make(chan *task.TaskSpec, BUF_TASK_CNT)
	ctx.reassignedTasks = make(chan *task.TaskSpec, BUF_TASK_CNT)
	ctx.jobMeta = new(JobMeta).Init()
	ctx.jobMeta.StartTs = time.Now()
}

func (ctx *JobCtx) finish() {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	ctx.jobMeta.EndTs = time.Now()
	ctx.jobMeta.Finished = true
}

func (ctx *JobCtx) assignJob(job task.Job) error {
	log.Info("Assign and init job %q", job.GetKind())
	if err := job.Init(); err != nil {
		err = fmt.Errorf("Fail to init job %q, %v", job.GetKind(), err)
		log.Error(err.Error())
		return err
	}
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	ctx.curJob = job
	ctx.jobMeta.setJob(job)
	return nil
}

func (ctx *JobCtx) setErr(err error) {
	log.Info("Set err %v to job ctx", err)
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	ctx.jobMeta.setErr(err)
	ctx.shouldFinish <- struct{}{}
}

func (ctx *JobCtx) aborted() bool {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	if ctx.jobMeta.getErr() != nil {
		return true
	} else {
		return false
	}
}

func (ctx *JobCtx) addTaskMeta(tspec *task.TaskSpec) {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	ctx.jobMeta.addTaskMeta(tspec)
}

func (ctx *JobCtx) getTaskMeta(tid string) *TaskMeta {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	return ctx.jobMeta.getTaskMeta(tid)
}

func (ctx *JobCtx) getTaskReports() []*task.TaskReport {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	reports := make([]*task.TaskReport, 0)
	for _, tmeta := range ctx.jobMeta.taskMetas {
		reports = append(reports, tmeta.report)
	}
	return reports
}

func (ctx *JobCtx) updateTaskMetaForWorker(tid, workerLabel string) {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	tmeta := ctx.jobMeta.getTaskMeta(tid)
	if tmeta == nil {
		log.Error("Task %q meta info not found", tid)
		return
	}
	tmeta.WorkerLabel = workerLabel
	tmeta.Dispatched = true
}

func (ctx *JobCtx) addTaskReport(report *task.TaskReport) {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	ctx.jobMeta.addTaskReport(report)
}

func (ctx *JobCtx) updateTaskStatus(status *task.TaskStatus) {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	ctx.jobMeta.updateTaskStatus(status)
}

func (ctx *JobCtx) getTaskErr(tid string) (int, string) {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	tmeta := ctx.jobMeta.getTaskMeta(tid)
	if tmeta == nil {
		log.Error("Task %q meta info not found", tid)
		return 0, ""
	}
	return tmeta.ErrCnt, tmeta.ErrMsg
}

func (ctx *JobCtx) incDispatched() {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	ctx.jobMeta.incDispatched()
}

func (ctx *JobCtx) incDone() {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	ctx.jobMeta.incDone()
	if ctx.jobMeta.allDone() {
		ctx.shouldFinish <- struct{}{}
	}
}

func (ctx *JobCtx) snapshotJobMeta() *JobMeta {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	if ctx.jobMeta == nil {
		return nil
	}
	return ctx.jobMeta.snapshot()
}

func taskStatusHandler(w http.ResponseWriter, r *http.Request) {
	key, err := getWorkerKeyFromReq(r)
	if err != nil {
		log.Error("Fail to get worker key, %v", err)
		server.FmtResp(w, err, nil)
		return
	}
	if err := wmgr.verifyWorkerKey(key); err != nil {
		log.Error("Fail on verify worker key, %v", err)
		server.FmtResp(w, err, nil)
		return
	}
	status := new(task.TaskStatus)
	if err := util.HttpFitRequestInto(r, status); err != nil {
		log.Error("Fail to read task status, %v", err)
		server.FmtResp(w, err, nil)
		return
	}
	jobctx.updateTaskStatus(status)
}

func taskReportHandler(w http.ResponseWriter, r *http.Request) {
	key, err := getWorkerKeyFromReq(r)
	if err != nil {
		log.Error("Fail to get worker key, %v", err)
		server.FmtResp(w, err, nil)
		return
	}
	if err := wmgr.verifyWorkerKey(key); err != nil {
		log.Error("Fail on verify worker key, %v", err)
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
	jobctx.addTaskReport(report)
	if report.Err != "" {
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
		workerName, err := wmgr.dispatchTask(t)
		if err != nil {
			jobctx.setErr(err)
			log.Error("Fail to dispatch task %q, exit dispatcher, %v", t.Tid, err)
			break
		}
		ctx.incDispatched()
		ctx.updateTaskMetaForWorker(t.Tid, workerName)
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
		case <-ctx.shouldFinish:
			return fmt.Errorf("Abort assign task, %v", ctx.jobMeta.getErr())
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
	case <-ctx.shouldFinish:
		break
	}
	close(ctx.todoTasks)
	close(ctx.reassignedTasks)
	err := ctx.jobMeta.getErr()
	log.Info("Job %q done, err %v", ctx.curJob.GetKind())
	return err
}

func generateTid(idx int) string {
	return fmt.Sprintf("tsk-%d-%d", time.Now().UnixNano(), idx)
}

func reduceTasks(ctx *JobCtx) error {
	log.Info("Reduce tasks for job")
	reports := ctx.getTaskReports()
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

func jobRunner(job task.Job) error {
	log.Info("Running job %q", job.GetKind())
	jobctx.start()
	if err := jobctx.assignJob(job); err != nil {
		return err
	}
	if jobctx.jobMeta.Total > 0 {
		if err := splitJobAndRun(job); err != nil {
			return err
		}
	}
	jobctx.finish()
	feedNextJobs(job)
	log.Info("Run job %q done", job.GetKind())
	return nil
}

func runJob(job task.Job) (*JobMeta, error) {
	err := jobRunner(job)
	if err != nil {
		jobctx.setErr(err)
	}
	return jobctx.snapshotJobMeta(), err
}
