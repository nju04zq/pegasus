package main

import (
	"fmt"
	"net/http"
	"pegasus/log"
	"pegasus/server"
	"pegasus/task"
	"pegasus/taskreg"
	"pegasus/uri"
	"pegasus/util"
	"sync"
	"time"
)

var projctx = new(ProjectCtx)

type ProjectCtx struct {
	idx int
	// Following fields under mutex protection
	mutex    sync.Mutex
	finished bool
	err      error
	free     bool
	startTs  time.Time
	endTs    time.Time
	pid      string
	proj     task.Project
}

func (ctx *ProjectCtx) start() {
	ctx.startTs = time.Now()
}

func (ctx *ProjectCtx) checkAndUnsetFree(proj task.Project) (string, error) {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	if !ctx.free {
		return "", fmt.Errorf("Project %q in running", ctx.pid)
	}
	ctx.free = false
	ctx.proj = proj
	ctx.pid = ctx.makeProjId()
	return ctx.pid, nil
}

func (ctx *ProjectCtx) setErr(err error) {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	ctx.err = err
	ctx.finished = true
	ctx.endTs = time.Now()
}

func (ctx *ProjectCtx) finish() {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	ctx.finished = true
	ctx.endTs = time.Now()
	// TODO need to save history data
	ctx.free = true
}

func (ctx *ProjectCtx) makeProjId() string {
	ts := time.Now().UnixNano()
	pid := fmt.Sprintf("proj%d-%d", ts, ctx.idx)
	ctx.idx++
	return pid
}

func projRunner() {
	log.Info("Run project %q", projctx.pid)
	projctx.start()
	proj := projctx.proj
	if err := proj.Init(); err != nil {
		projctx.setErr(err)
		log.Info("Fail on project %q init, %v", projctx.pid, err)
		return
	}
	for _, job := range proj.GetJobs() {
		err := runJob(job)
		if err != nil {
			err = fmt.Errorf("Fail on job %q, %v", job.GetKind(), err)
			projctx.setErr(err)
			break
		}
	}
	if err := proj.Finish(); err != nil {
		projctx.setErr(err)
		log.Info("Fail on project %q finish, %v", projctx.pid, err)
		return
	}
	projctx.finish()
	log.Info("Run project %q finished", projctx.pid)
}

func runProj(proj task.Project) (string, error) {
	pid, err := projctx.checkAndUnsetFree(proj)
	if err != nil {
		return "", err
	}
	go projRunner()
	return pid, nil
}

func runProjHandler(w http.ResponseWriter, r *http.Request) {
	key := uri.MasterProjNameKey
	projName, err := util.GetFormValFromReq(r, key)
	if err != nil {
		err = fmt.Errorf("Fail to get key %q value from request, err %v", key, err)
		server.FmtResp(w, err, nil)
		return
	}
	proj := taskreg.GetProj(projName)
	if proj == nil {
		err = fmt.Errorf("Proj %q not supported", projName)
		server.FmtResp(w, err, nil)
		return
	}
	pid, err := runProj(proj)
	server.FmtResp(w, err, pid)
}

func init() {
	projctx.free = true
}
