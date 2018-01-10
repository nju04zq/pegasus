package main

import (
	"fmt"
	"net/http"
	"pegasus/log"
	"pegasus/server"
	"pegasus/task"
	"pegasus/uri"
	"pegasus/util"
	"sync"
	"time"
)

const (
	WORKER_STATUS_ACTIVE   = "Active"
	WORKER_STATUS_PENDING  = "Pending"
	WORKER_STATUS_UNSTABLE = "Unstable"
	WORKER_STATUS_DEAD     = "Dead"
	WORKER_STATUS_FAULT    = "Fault"
	WORKER_STATUS_REMOVED  = "Removed"
)

const (
	WORKER_HB_INTERVAL      = time.Duration(5 * time.Second)
	WORKER_MONITOR_INTERVAL = time.Duration(30 * time.Second)
	WORKER_HB_CNT_GOOD      = 5
	WORKER_HB_CNT_NORM      = 3
)

var wmgr = new(workerMgr)

type Worker struct {
	Addr        string
	ip          string
	port        int
	Key         string
	Status      string
	StatusStart time.Time
	LastHb      time.Time
	HbWinCnt    int
	FaultCnt    int
	tspec       *task.TaskSpec
	doneTasks   int
	prev        *Worker
	next        *Worker
	listHead    **Worker
}

type workerMgr struct {
	mutex           *sync.Mutex
	cond            *sync.Cond
	regNum          int
	workers         map[string]*Worker
	freeWorkers     *Worker
	busyWorkers     *Worker
	unstableWorkers *Worker
	faultWorkers    *Worker
	deadWorkers     *Worker
}

func (mgr *workerMgr) init() {
	mgr.workers = make(map[string]*Worker)
	mgr.mutex = new(sync.Mutex)
	mgr.cond = sync.NewCond(mgr.mutex)
}

func (mgr *workerMgr) getAllWorkerAddr() (ips []string, ports []int) {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()
	if len(mgr.workers) == 0 {
		return
	}
	for _, w := range mgr.workers {
		ips = append(ips, w.ip)
		ports = append(ports, w.port)
	}
	return
}

func (mgr *workerMgr) removeWorker(worker *Worker) {
	if _, ok := mgr.workers[worker.Key]; ok {
		delete(mgr.workers, worker.Key)
	}
	if worker.listHead != nil {
		mgr.removeFrom(worker, worker.listHead)
	}
}

func (mgr *workerMgr) registerWorker() (key string, err error) {
	mgr.mutex.Lock()
	defer func() {
		mgr.mutex.Unlock()
		log.Info("Get worker key as %q, %v", key, err)
	}()
	key = fmt.Sprintf("%d-%d", time.Now().UnixNano(), mgr.regNum)
	mgr.regNum++
	worker := &Worker{
		Key:         key,
		Status:      WORKER_STATUS_PENDING,
		StatusStart: time.Now(),
	}
	mgr.workers[key] = worker
	return
}

func (mgr *workerMgr) verifyWorker(addr, key string) (err error) {
	mgr.mutex.Lock()
	defer func() {
		mgr.mutex.Unlock()
		log.Info("Verify worker %q, key %q, err %v", addr, key, err)
	}()
	worker, ok := mgr.workers[key]
	if !ok {
		err := fmt.Errorf("Worker key %s not registered", key)
		return err
	}
	ip, port, err := util.SplitAddr(addr)
	if err != nil {
		err = fmt.Errorf("Fail to split addr, %v", err)
		return
	}
	worker.Addr, worker.ip, worker.port = addr, ip, port
	worker.StatusStart = time.Now()
	mgr.insertWorker(worker, &mgr.unstableWorkers)
	worker.Status = WORKER_STATUS_UNSTABLE
	return nil
}

func (mgr *workerMgr) insertWorker(worker *Worker, head **Worker) {
	if *head == nil {
		worker.prev, worker.next = worker, worker
		*head = worker
	} else {
		worker.prev = (*head).prev
		worker.next = (*head)
		(*head).prev.next = worker
		(*head).prev = worker
	}
	worker.listHead = head
	if head == &mgr.freeWorkers {
		mgr.cond.Broadcast()
	}
}

func (mgr *workerMgr) removeFrom(worker *Worker, head **Worker) {
	if worker.prev == worker {
		*head = nil
	} else {
		if *head == worker {
			*head = worker.next
		}
		worker.prev.next = worker.next
		worker.next.prev = worker.prev
	}
	worker.listHead = nil
}

func (mgr *workerMgr) reinsertWorker(worker *Worker, head **Worker) {
	mgr.removeFrom(worker, worker.listHead)
	mgr.insertWorker(worker, head)
}

func (mgr *workerMgr) removeFirstOneFrom(head **Worker) *Worker {
	worker := (*head).next
	if (*head).next == (*head).prev {
		*head = nil
	}
	worker.listHead = nil
	return worker
}

func (mgr *workerMgr) getFreeWorker() (*Worker, error) {
	log.Info("Get free worker...")
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()
	if len(mgr.workers) == 0 {
		return nil, fmt.Errorf("No workers registered")
	}
	for mgr.freeWorkers == nil {
		mgr.cond.Wait()
	}
	worker := mgr.removeFirstOneFrom(&mgr.freeWorkers)
	return worker, nil
}

func (mgr *workerMgr) dispatchTask(t *task.TaskSpec) error {
	log.Info("Dispatch task %q", t.Tid)
	worker, err := mgr.getFreeWorker()
	if err != nil {
		return fmt.Errorf("Fail to get free worker, %v", err)
	}
	url := &util.HttpUrl{
		IP:   worker.ip,
		Port: worker.port,
		Uri:  uri.CfgMasterUri,
	}
	if _, err := util.HttpPostData(url, t); err != nil {
		return fmt.Errorf("Fail to post task spec to %q, %v", worker.Addr, err)
	}
	log.Info("Dispatch task %q successfully", t.Tid)
	return nil
}

func (mgr *workerMgr) releaseWorker() {

}

func (mgr *workerMgr) handleTaskReport(ctx *JobCtx, addr string, report *task.TaskReport) error {
	return nil
}

func (mgr *workerMgr) updateWorkerHb(key string, ts time.Time) (err error) {
	log.Info("Update HB for worker %q", key)
	mgr.mutex.Lock()
	defer func() {
		mgr.mutex.Unlock()
		log.Info("Update HB for worker %q done, err %v", key, err)
	}()
	w, ok := mgr.workers[key]
	if !ok {
		err = fmt.Errorf("Worker with key %q not found", key)
		return
	}
	w.HbWinCnt++
	w.LastHb = ts
	return
}

func registerWorkerHandler(w http.ResponseWriter, r *http.Request) {
	key, err := wmgr.registerWorker()
	server.FmtResp(w, err, key)
}

func getWorkerKeyFromReq(r *http.Request) (string, error) {
	if err := r.ParseForm(); err != nil {
		return "", fmt.Errorf("Fail to parse form, %v", err)
	}
	key := r.Form.Get(uri.MasterWorkerQueryKey)
	return key, nil
}

func verifyWorkerHandler(w http.ResponseWriter, r *http.Request) {
	addr, err := util.HttpReadRequestTextBody(r)
	if err != nil {
		server.FmtResp(w, err, "")
		return
	}
	key, err := getWorkerKeyFromReq(r)
	if err != nil {
		log.Error("Fail to get worker key from request, %v", err)
		server.FmtResp(w, err, "")
		return
	}
	err = wmgr.verifyWorker(addr, key)
	server.FmtResp(w, err, "")
}

func workerHbHandler(w http.ResponseWriter, r *http.Request) {
	key, err := getWorkerKeyFromReq(r)
	if err != nil {
		log.Error("Fail to get worker key from request, %v", err)
		server.FmtResp(w, err, "")
		return
	}
	err = wmgr.updateWorkerHb(key, time.Now())
	server.FmtResp(w, err, "")
}

func workerHbIntervalHandler(w http.ResponseWriter, r *http.Request) {
	interval := WORKER_HB_INTERVAL
	server.FmtResp(w, nil, &interval)
}

type monitorRec struct {
	workerKey  string
	workerAddr string
	oldStatus  string
	newStatus  string
}

func monitorGoodWorker(w *Worker) *monitorRec {
	if w.Status == WORKER_STATUS_ACTIVE {
		return nil
	}
	if w.Status != WORKER_STATUS_UNSTABLE {
		return nil
	}
	rec := &monitorRec{
		workerKey:  w.Key,
		workerAddr: w.Addr,
		oldStatus:  w.Status,
		newStatus:  WORKER_STATUS_ACTIVE,
	}
	w.Status = WORKER_STATUS_ACTIVE
	if w.tspec == nil {
		wmgr.reinsertWorker(w, &wmgr.freeWorkers)
	}
	return rec
}

func monitorBadWorker(w *Worker) *monitorRec {
	oldStatus := w.Status
	if w.Status == WORKER_STATUS_ACTIVE {
		w.Status = WORKER_STATUS_UNSTABLE
		if w.tspec == nil {
			wmgr.reinsertWorker(w, &wmgr.unstableWorkers)
		}
	} else if w.Status == WORKER_STATUS_UNSTABLE {
		if w.tspec != nil {
			reassignTask(w.tspec)
		}
		w.Status, w.tspec = WORKER_STATUS_DEAD, nil
		wmgr.reinsertWorker(w, &wmgr.deadWorkers)
	} else {
		return nil
	}
	rec := &monitorRec{
		workerKey:  w.Key,
		workerAddr: w.Addr,
		oldStatus:  oldStatus,
		newStatus:  w.Status,
	}
	return rec
}

func monitorOneWorker(w *Worker) *monitorRec {
	var rec *monitorRec
	d := time.Now().Sub(w.StatusStart)
	if d < WORKER_MONITOR_INTERVAL {
		return nil
	}
	if w.Status == WORKER_STATUS_FAULT {
		return nil
	}
	if w.Status == WORKER_STATUS_PENDING || w.Status == WORKER_STATUS_DEAD {
		wmgr.removeWorker(w)
		return &monitorRec{
			workerKey:  w.Key,
			workerAddr: w.Addr,
			oldStatus:  w.Status,
			newStatus:  WORKER_STATUS_REMOVED,
		}
	}
	if w.HbWinCnt >= WORKER_HB_CNT_GOOD {
		rec = monitorGoodWorker(w)
	} else if w.HbWinCnt < WORKER_HB_CNT_NORM {
		rec = monitorBadWorker(w)
	}
	w.HbWinCnt = 0
	return rec
}

func monitorWorkers() []*monitorRec {
	wmgr.mutex.Lock()
	defer wmgr.mutex.Unlock()
	recs := make([]*monitorRec, 0)
	keys := make([]string, len(wmgr.workers))
	i := 0
	for key, _ := range wmgr.workers {
		keys[i] = key
		i++
	}
	for _, key := range keys {
		if rec := monitorOneWorker(wmgr.workers[key]); rec != nil {
			recs = append(recs, rec)
		}
	}
	return recs
}

func wmgrMonitorMain(args interface{}) {
	log.Info("Start WMGR monitor")
	t1 := time.Now()
	recs := monitorWorkers()
	t2 := time.Now()
	tbl := new(util.PrettyTable)
	tbl.Init([]string{"Key", "Addr", "From", "To"})
	for _, rec := range recs {
		tbl.AppendLine([]string{rec.workerKey, rec.workerAddr,
			rec.oldStatus, rec.newStatus})
	}
	log.Info("WMGR monitor took %v, records:\n%s", t2.Sub(t1), tbl.Format())
}

func init() {
	wmgr.init()
	go util.PeriodicalRoutine(true, WORKER_MONITOR_INTERVAL, wmgrMonitorMain, nil)
}
