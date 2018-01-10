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
)

var tskctx = &TaskCtx{}

const (
	BUF_TASKLET_CNT     = 8
	RUNNING_TASKLET_CNT = 4
	TASKLET_RETRY_CNT   = 3
)

type TaskCtx struct {
	tsk            task.Task
	wgFinish       sync.WaitGroup
	taskletCtxList []task.TaskletCtx
	todoTasklets   chan task.Tasklet
	doneTasklets   chan task.Tasklet
	// Following fields under mutex protection
	mutex sync.Mutex
	free  bool
	err   error
}

func (ctx *TaskCtx) init() {
	ctx.todoTasklets = make(chan task.Tasklet, BUF_TASKLET_CNT)
	ctx.doneTasklets = make(chan task.Tasklet)
	ctx.err = nil
}

func (ctx *TaskCtx) aborted() bool {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	if ctx.err == nil {
		return false
	} else {
		return true
	}
}

func (ctx *TaskCtx) setErr(err error) {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	ctx.err = err
}

func (ctx *TaskCtx) checkAndSetFree() error {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	if !ctx.free {
		return fmt.Errorf("Worker busy with task %q", ctx.tsk.GetTaskKind())
	}
	ctx.free = false
	return nil
}

func getTaskletCnt() int {
	return RUNNING_TASKLET_CNT
}

func prepareExecutors(ctx *TaskCtx, tsk task.Task) {
	taskletCnt := getTaskletCnt()
	for i := 0; i <= taskletCnt; i++ {
		c := tsk.NewTaskletCtx()
		ctx.wgFinish.Add(1)
		go taskletExecutor(ctx, c)
		ctx.taskletCtxList = append(ctx.taskletCtxList, c)
	}
}

func releaseExecutors(ctx *TaskCtx) {
	for _, c := range ctx.taskletCtxList {
		c.Close()
	}
}

func handleTaskReq(tspec *task.TaskSpec) {
	tskctx.init()
	tsk := spawnTask(tspec)
	prepareExecutors(tskctx, tsk)
	assignTasklets(tskctx, tsk)
	tskctx.wgFinish.Wait()
	releaseExecutors(tskctx)
	if tskctx.aborted() {
		tsk.SetError(tskctx.err)
	} else {
		reduceTasklets(tsk, tskctx)
	}
	report := task.GenerateTaskReport(tsk)
	sendTaskReport(report)
}

func spawnTask(tspec *task.TaskSpec) task.Task {
	gen := task.GetTaskGenerator(tspec.Kind)
	if gen == nil {
		return nil
	}
	return gen(tspec)
}

func assignTasklets(ctx *TaskCtx, tsk task.Task) {
	log.Info("Assign tasklets")
	for {
		if ctx.aborted() {
			log.Info("Abort assign tasklets")
			break
		}
		tasklet := tsk.GetNextTasklet()
		if tasklet == nil {
			close(ctx.todoTasklets)
			break
		}
		log.Info("Put tasklet %q to todo list", tasklet.GetTaskletId())
		ctx.todoTasklets <- tasklet
	}
	log.Info("Assign tasklets finished")
}

func taskletExecutor(ctx *TaskCtx, c task.TaskletCtx) {
	var err error
	defer ctx.wgFinish.Done()
	for {
		if ctx.aborted() {
			log.Info("Error set in taskctx, abort executor")
			break
		}
		tasklet, ok := <-ctx.todoTasklets
		if !ok {
			log.Info("Todo tasklets drained, exit executor")
			break
		}
		for i := 0; i < TASKLET_RETRY_CNT; i++ {
			if err = tasklet.Execute(c); err == nil {
				break
			}
			log.Info("Retry execute tasklet %q", tasklet.GetTaskletId())
		}
		if err != nil {
			log.Info("Fail on tasklet %q, err %v", tasklet.GetTaskletId(), err)
			tskctx.setErr(err)
			break
		}
		ctx.doneTasklets <- tasklet
	}
}

func reduceTasklets(tsk task.Task, ctx *TaskCtx) {
	close(ctx.doneTasklets)
	for {
		tasklet, ok := <-ctx.doneTasklets
		if !ok {
			break
		}
		tsk.ReduceTasklet(tasklet)
	}
}

func sendTaskReport(report *task.TaskReport) {
	url := &util.HttpUrl{
		//TODO
		IP:   "",
		Port: 0,
		Uri:  "",
	}
	util.HttpPostData(url, report)
}

func makeTaskspec(r *http.Request) (tspec *task.TaskSpec, err error) {
	buf, err := util.HttpReadRequestJsonBody(r)
	if err != nil {
		err = fmt.Errorf("Fail to read request body, %v", err)
		return
	}
	tspec = new(task.TaskSpec)
	if err = json.Unmarshal(buf, tspec); err != nil {
		err = fmt.Errorf("Fail unmarshal tspec %s, %v", string(buf), err)
		return
	}
	return
}

func taskRecepiant(tspec *task.TaskSpec) error {
	if err := tskctx.checkAndSetFree(); err != nil {
		return err
	}
	go handleTaskReq(tspec)
	return nil
}

func taskRecipiantHandler(w http.ResponseWriter, r *http.Request) {
	tspec, err := makeTaskspec(r)
	if err != nil {
		server.FmtResp(w, err, "")
		return
	}
	err = taskRecepiant(tspec)
	server.FmtResp(w, err, "")
}
