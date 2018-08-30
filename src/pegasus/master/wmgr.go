package main

import (
	"bytes"
	"fmt"
	"net/http"
	"pegasus/log"
	"pegasus/server"
	"pegasus/task"
	"pegasus/uri"
	"pegasus/util"
	"pegasus/workgroup"
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
	WORKER_MAX_FAULT        = 2
)

var wmgr = new(workerMgr)

type Worker struct {
	Label       string
	Name        string
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
	label := fmt.Sprintf("Worker#%03d", mgr.regNum)
	mgr.regNum++
	worker := &Worker{
		Label:       label,
		Key:         key,
		Status:      WORKER_STATUS_PENDING,
		StatusStart: time.Now(),
	}
	mgr.workers[key] = worker
	return
}

func (mgr *workerMgr) verifyWorker(form *workgroup.WorkerRegForm, key string) (err error) {
	mgr.mutex.Lock()
	defer func() {
		mgr.mutex.Unlock()
		log.Info("Verify worker %q, key %q, err %v", form.Name, key, err)
	}()
	worker, ok := mgr.workers[key]
	if !ok {
		err := fmt.Errorf("Worker key %s not registered", key)
		return err
	}
	worker.Name = form.Name
	worker.ip, worker.port = form.IP, form.Port
	worker.StatusStart = time.Now()
	// TODO for test purpose
	//mgr.insertWorker(worker, &mgr.unstableWorkers)
	//worker.Status = WORKER_STATUS_UNSTABLE
	mgr.insertWorker(worker, &mgr.freeWorkers)
	worker.Status = WORKER_STATUS_ACTIVE
	return nil
}

func (mgr *workerMgr) verifyWorkerKey(key string) error {
	mgr.mutex.Lock()
	mgr.mutex.Unlock()
	if _, ok := mgr.workers[key]; !ok {
		return fmt.Errorf("Worker key %s not registered", key)
	}
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

func (mgr *workerMgr) insertWorkerInlock(worker *Worker, head **Worker) {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()
	mgr.insertWorker(worker, head)
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

func (mgr *workerMgr) dumpWorkerList(head *Worker) {
	buf := bytes.NewBuffer(nil)
	w := head
	for w != nil {
		buf.WriteString(fmt.Sprintf("%q -> ", w.Key))
		w = w.next
		if w == head {
			break
		}
	}
	log.Info("workers: %s", buf.String())
}

func (mgr *workerMgr) removeFirstOneFrom(head **Worker) *Worker {
	worker := *head
	if worker.next == worker {
		*head = nil
	} else {
		*head = worker.next
		worker.next.prev = worker.prev
		worker.prev.next = worker.next
	}
	worker.next, worker.prev, worker.listHead = nil, nil, nil
	return worker
}

func (mgr *workerMgr) waitForFreeWorker() error {
	for mgr.freeWorkers == nil {
		// TODO should we keep track of avail workers count???
		if len(mgr.workers) == 0 {
			return fmt.Errorf("No workers registered or all workers dead")
		}
		mgr.cond.Wait()
	}
	return nil
}

func (mgr *workerMgr) notifyFreeWorker() {
	mgr.cond.Broadcast()
}

func (mgr *workerMgr) getFreeWorker() (*Worker, error) {
	log.Info("Get free worker...")
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()
	if len(mgr.workers) == 0 {
		return nil, fmt.Errorf("No workers registered or all workers dead")
	}
	if err := mgr.waitForFreeWorker(); err != nil {
		return nil, err
	}
	worker := mgr.removeFirstOneFrom(&mgr.freeWorkers)
	return worker, nil
}

func (mgr *workerMgr) dispatchTaskTo(t *task.TaskSpec, w *Worker) error {
	log.Info("Post task %q to worker %v", t.Tid, w.Key)
	url := &util.HttpUrl{
		IP:   w.ip,
		Port: w.port,
		Uri:  uri.WorkerTaskUri,
	}
	if _, err := util.HttpPostData(url, t); err != nil {
		return fmt.Errorf("Fail to post task spec to %q, %v", w.Name, err)
	}
	w.tspec = t
	log.Info("Post task %q done", t.Tid)
	return nil
}

func (mgr *workerMgr) dispatchTask(t *task.TaskSpec) (string, error) {
	var err error
	var w *Worker
	log.Info("Dispatch task %q", t.Tid)
	for {
		w, err = mgr.getFreeWorker()
		if err != nil {
			return "", fmt.Errorf("Fail to get free worker, %v", err)
		}
		if err := mgr.dispatchTaskTo(t, w); err == nil {
			mgr.insertWorkerInlock(w, &mgr.busyWorkers)
			break
		} else {
			log.Error("Fail to dispatch task to %q, %v", w.Key, err)
		}
		w.Status = WORKER_STATUS_UNSTABLE
		mgr.insertWorkerInlock(w, &mgr.unstableWorkers)
	}
	log.Info("Dispatch task %q successfully to %s", t.Tid, w.Name)
	return w.Label, nil
}

func (mgr *workerMgr) releaseWorker(w *Worker) (logMsg string) {
	w.tspec = nil
	if w.FaultCnt >= WORKER_MAX_FAULT {
		w.Status = WORKER_STATUS_FAULT
		// TODO should we remove it???
		mgr.reinsertWorker(w, &mgr.faultWorkers)
		logMsg = fmt.Sprintf("Woker %q fault %d, move to fault queue", w.Key, w.FaultCnt)
	} else if w.Status == WORKER_STATUS_UNSTABLE {
		w.Status = WORKER_STATUS_UNSTABLE
		mgr.reinsertWorker(w, &mgr.unstableWorkers)
		logMsg = fmt.Sprintf("Worker %q released to unstable queue", w.Key)
	} else {
		w.Status = WORKER_STATUS_ACTIVE
		mgr.reinsertWorker(w, &mgr.freeWorkers)
		logMsg = fmt.Sprintf("Worker %q released to free queue", w.Key)
	}
	mgr.notifyFreeWorker()
	return
}

func (mgr *workerMgr) handleTaskReport(ctx *JobCtx, key string, report *task.TaskReport) error {
	var logMsg string
	log.Info("Handle task report %q from %q, report err, %v", report.Tid, key, report.Err)
	mgr.mutex.Lock()
	defer func() {
		mgr.mutex.Unlock()
		log.Info("Handle task report %q result: %s", report.Tid, logMsg)
	}()
	w, ok := mgr.workers[key]
	if !ok {
		return fmt.Errorf("Worker with key %q not found", key)
	}
	if report.Err != "" {
		w.FaultCnt++
	} else {
		w.doneTasks++
	}
	logMsg = mgr.releaseWorker(w)
	return nil
}

func (mgr *workerMgr) updateWorkerHb(key string, ts time.Time) (err error) {
	log.Debug("Update HB for worker %q", key)
	mgr.mutex.Lock()
	defer func() {
		mgr.mutex.Unlock()
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
	return r.Form.Get(uri.MasterWorkerQueryKey), nil
}

func verifyWorkerHandler(w http.ResponseWriter, r *http.Request) {
	form := new(workgroup.WorkerRegForm)
	if err := util.HttpFitRequestInto(r, form); err != nil {
		log.Error("Fail to get reg form from request, %v", err)
		server.FmtResp(w, err, "")
		return
	}
	key, err := getWorkerKeyFromReq(r)
	if err != nil {
		log.Error("Fail to get worker key from request, %v", err)
		server.FmtResp(w, err, "")
		return
	}
	err = wmgr.verifyWorker(form, key)
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
	workerName string
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
		workerName: w.Name,
		oldStatus:  w.Status,
		newStatus:  WORKER_STATUS_ACTIVE,
	}
	w.Status = WORKER_STATUS_ACTIVE
	if w.tspec == nil {
		wmgr.reinsertWorker(w, &wmgr.freeWorkers)
		wmgr.notifyFreeWorker()
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
			go reassignTask(w.tspec)
		}
		w.Status, w.tspec = WORKER_STATUS_DEAD, nil
		wmgr.reinsertWorker(w, &wmgr.deadWorkers)
		wmgr.notifyFreeWorker()
	} else {
		return nil
	}
	rec := &monitorRec{
		workerKey:  w.Key,
		workerName: w.Name,
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
			workerName: w.Name,
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
	log.Debug("Start WMGR monitor")
	t1 := time.Now()
	recs := monitorWorkers()
	t2 := time.Now()
	log.Debug("WMGR monitor done")
	tbl := new(util.PrettyTable)
	tbl.Init([]string{"Key", "Name", "From", "To"})
	for _, rec := range recs {
		tbl.AppendLine([]string{rec.workerKey, rec.workerName,
			rec.oldStatus, rec.newStatus})
	}
	log.Debug("WMGR monitor took %v, records:\n%s", t2.Sub(t1), tbl.Format())
}

func init() {
	wmgr.init()
	go util.PeriodicalRoutine(true, WORKER_MONITOR_INTERVAL, wmgrMonitorMain, nil)
}
