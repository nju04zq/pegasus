package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"pegasus/lianjia"
	"pegasus/uri"
	"pegasus/util"
	"sync"
	"time"

	"github.com/mattn/go-isatty"
)

const (
	MONITOR_STATUS_INTERVAL = 1 * time.Second
	SHOW_STATUS_INTERVAL    = 1 * time.Second
)

func isTerminal() bool {
	return isatty.IsTerminal(os.Stdout.Fd())
}

type ProjMeta struct {
	Name     string
	StartTs  time.Time
	EndTs    time.Time
	ErrMsg   string
	Finished bool
	JobMetas []*JobMeta
}

type JobMeta struct {
	JobId      string
	Kind       string
	StartTs    time.Time
	EndTs      time.Time
	ErrMsg     string
	Finished   bool
	Total      int
	Dispatched int
	Done       int
	Report     string
	TaskMetas  []*TaskMeta
}

type TaskMeta struct {
	Kind       string
	Desc       string
	StartTs    time.Time
	EndTs      time.Time
	ErrMsg     string
	Total      int
	Done       int
	Dispatched bool
	Finished   bool
}

func formatSeconds(seconds int) string {
	minutes := seconds / 60
	seconds = seconds % 60
	return fmt.Sprintf("%02d:%02d", minutes, seconds)
}

func formatTs(ts time.Time) string {
	return ts.Format("15:04:05")
}

func formatJmetaTs(jmeta *JobMeta) string {
	return formatTs(jmeta.StartTs)
}

func formatHeader(jmeta *JobMeta, header string) string {
	ts := formatJmetaTs(jmeta)
	return blueColorText(ts) + " " + boldText(header)
}

func boldText(text string) string {
	if isTerminal() {
		return "\033[1m" + text + "\033[0m"
	} else {
		return text
	}
}

func blueColorText(text string) string {
	if isTerminal() {
		return "\033[34m" + text + "\033[0m"
	} else {
		return text
	}
}

func redColorText(text string) string {
	if isTerminal() {
		return "\033[31m" + text + "\033[0m"
	} else {
		return text
	}
}

type projEvent interface {
	getMeta() *JobMeta
	setMeta(meta *JobMeta)
	getHeader() string
	showProgress() bool
	getProgress() string
	getReport() string
}

type getDistrictEvent struct {
	jmeta *JobMeta
}

func (evt *getDistrictEvent) getMeta() *JobMeta {
	return evt.jmeta
}

func (evt *getDistrictEvent) setMeta(jmeta *JobMeta) {
	evt.jmeta = jmeta
}

func (evt *getDistrictEvent) getHeader() string {
	return formatHeader(evt.jmeta, "Get districts...")
}

func (evt *getDistrictEvent) showProgress() bool {
	return false
}
func (evt *getDistrictEvent) getProgress() string {
	return ""
}

func (evt *getDistrictEvent) getReport() string {
	return evt.jmeta.Report
}

type getRegionEvent struct {
	jmeta *JobMeta
}

func (evt *getRegionEvent) getMeta() *JobMeta {
	return evt.jmeta
}

func (evt *getRegionEvent) setMeta(jmeta *JobMeta) {
	evt.jmeta = jmeta
}

func (evt *getRegionEvent) getHeader() string {
	return formatHeader(evt.jmeta, "Get regions...")
}

func (evt *getRegionEvent) showProgress() bool {
	return false
}

func (evt *getRegionEvent) getProgress() string {
	return ""
}

func (evt *getRegionEvent) getReport() string {
	return evt.jmeta.Report
}

type getRegionMaxpageEvent struct {
	jmeta *JobMeta
}

func (evt *getRegionMaxpageEvent) getMeta() *JobMeta {
	return evt.jmeta
}

func (evt *getRegionMaxpageEvent) setMeta(jmeta *JobMeta) {
	evt.jmeta = jmeta
}

func (evt *getRegionMaxpageEvent) getHeader() string {
	return formatHeader(evt.jmeta, "Get region max page...")
}
func (evt *getRegionMaxpageEvent) showProgress() bool {
	return true
}

func (evt *getRegionMaxpageEvent) getProgress() string {
	jmeta := evt.jmeta
	percent := float32(jmeta.Done) / float32(jmeta.Total) * 100
	var end time.Time
	if jmeta.Finished {
		end = jmeta.EndTs
	} else {
		end = time.Now()
	}
	seconds := int(end.Sub(jmeta.StartTs) / time.Second)
	return fmt.Sprintf("%0.2f%% [%d/%d] %s", percent, jmeta.Done, jmeta.Total, formatSeconds(seconds))
}

func (evt *getRegionMaxpageEvent) getReport() string {
	return evt.jmeta.Report
}

type getApartmentsEvent struct {
	jmeta *JobMeta
}

func (evt *getApartmentsEvent) getMeta() *JobMeta {
	return evt.jmeta
}

func (evt *getApartmentsEvent) setMeta(jmeta *JobMeta) {
	evt.jmeta = jmeta
}

func (evt *getApartmentsEvent) getHeader() string {
	return formatHeader(evt.jmeta, "Get apartments...")
}

func (evt *getApartmentsEvent) showProgress() bool {
	return true
}

func (evt *getApartmentsEvent) getDetail() string {
	jmeta := evt.jmeta
	if len(jmeta.TaskMetas) == 0 {
		return "<None>"
	}
	buf := bytes.NewBuffer(nil)
	for i := 0; i < len(jmeta.TaskMetas); i++ {
		tmeta := jmeta.TaskMetas[i]
		if tmeta.Done == tmeta.Total {
			continue
		}
		percent := 100 * tmeta.Done / tmeta.Total
		buf.WriteString(fmt.Sprintf("%s %d%% ", tmeta.Desc, percent))
	}
	return buf.String()
}

func (evt *getApartmentsEvent) getProgress() string {
	jmeta := evt.jmeta
	percent := float32(jmeta.Done) / float32(jmeta.Total) * 100
	progress := fmt.Sprintf("%0.1f%% [%d/%d]", percent, jmeta.Done, jmeta.Total)
	detail := evt.getDetail()
	var end time.Time
	if jmeta.Finished {
		end = jmeta.EndTs
	} else {
		end = time.Now()
	}
	seconds := int(end.Sub(jmeta.StartTs) / time.Second)
	return fmt.Sprintf("%s %s %s", progress, detail, formatSeconds(seconds))
}

func (evt *getApartmentsEvent) getReport() string {
	return evt.jmeta.Report
}

type updateDbEvent struct {
	jmeta *JobMeta
}

func (evt *updateDbEvent) getMeta() *JobMeta {
	return evt.jmeta
}

func (evt *updateDbEvent) setMeta(jmeta *JobMeta) {
	evt.jmeta = jmeta
}

func (evt *updateDbEvent) getHeader() string {
	return formatHeader(evt.jmeta, "Update database...")
}

func (evt *updateDbEvent) showProgress() bool {
	return true
}

func (evt *updateDbEvent) getProgress() string {
	jmeta := evt.jmeta
	percent := float32(jmeta.Done) / float32(jmeta.Total) * 100
	var end time.Time
	if jmeta.Finished {
		end = jmeta.EndTs
	} else {
		end = time.Now()
	}
	seconds := int(end.Sub(jmeta.StartTs) / time.Second)
	return fmt.Sprintf("%0.2f%% [%d/%d] %s", percent, jmeta.Done, jmeta.Total, formatSeconds(seconds))
}

func (evt *updateDbEvent) getReport() string {
	return evt.jmeta.Report
}

type progressMgr struct {
	ip        string
	port      int
	projId    string
	pmeta     *ProjMeta
	mutex     sync.Mutex
	alignIdx  int
	events    []projEvent
	lastJobid string
	lastLine  string
}

func (mgr *progressMgr) init(ip string, port int, projId string) *progressMgr {
	mgr.ip, mgr.port, mgr.projId = ip, port, projId
	mgr.pmeta = new(ProjMeta)
	return mgr
}

func (mgr *progressMgr) fetch() error {
	u := &util.HttpUrl{
		IP:    mgr.ip,
		Port:  mgr.port,
		Uri:   uri.MasterProjectStatusUri,
		Query: make(url.Values),
	}
	data, err := util.HttpGet(u)
	if err != nil {
		return err
	}
	saveTo(data)
	if err := json.Unmarshal([]byte(data), mgr.pmeta); err != nil {
		return fmt.Errorf("Fail to unmarshal proj status, %v", err)
	}
	return nil
}

func (mgr *progressMgr) handleEvents() error {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()
	if len(mgr.events) > 0 {
		if err := mgr.updateEvents(); err != nil {
			return err
		}
	}
	mgr.insertEvents()
	return nil
}

func (mgr *progressMgr) updateEvents() error {
	i := len(mgr.events) - 1
	j := i + mgr.alignIdx
	if j >= len(mgr.pmeta.JobMetas) {
		return fmt.Errorf("Align idx exceed bounds")
	}
	mgr.events[i].setMeta(mgr.pmeta.JobMetas[j])
	return nil
}

func (mgr *progressMgr) insertEvents() {
	i := mgr.alignIdx + len(mgr.events)
	for ; i < len(mgr.pmeta.JobMetas); i++ {
		event := mgr.newEvent(mgr.pmeta.JobMetas[i])
		mgr.events = append(mgr.events, event)
	}
}

func (mgr *progressMgr) newEvent(jmeta *JobMeta) projEvent {
	var evt projEvent
	switch jmeta.Kind {
	case lianjia.JOB_KIND_DISTRICTS:
		evt = new(getDistrictEvent)
	case lianjia.JOB_KIND_REGIONS:
		evt = new(getRegionEvent)
	case lianjia.JOB_KIND_REGION_MAXPAGE:
		evt = new(getRegionMaxpageEvent)
	case lianjia.JOB_KIND_GET_APARTMENTS:
		evt = new(getApartmentsEvent)
	case lianjia.JOB_KIND_UPDATE_DB:
		evt = new(updateDbEvent)
	default:
		panic(fmt.Errorf("Unrecognized job kind %q", jmeta.Kind))
	}
	evt.setMeta(jmeta)
	return evt
}

func (mgr *progressMgr) update() error {
	if err := mgr.fetch(); err != nil {
		return err
	}
	if err := mgr.handleEvents(); err != nil {
		return err
	}
	return nil
}

func (mgr *progressMgr) show() bool {
	msg, finished := mgr.formatShowMsg()
	mgr.clearLastLine()
	os.Stdout.WriteString(msg)
	mgr.lastLine = mgr.getLastLine(msg)
	return finished
}

func (mgr *progressMgr) formatShowMsg() (string, bool) {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()
	if len(mgr.events) == 0 {
		return "", false
	}
	buf := bytes.NewBuffer(nil)
	for i := 0; i < len(mgr.events)-1; i++ {
		evt := mgr.events[i]
		if i > 0 || (i == 0 && evt.getMeta().JobId != mgr.lastJobid) {
			buf.WriteString(evt.getHeader() + "\n")
		}
		buf.WriteString(evt.getReport() + "\n")
	}
	lastEvt := mgr.events[len(mgr.events)-1]
	lastJmeta := lastEvt.getMeta()
	if lastJmeta.JobId != mgr.lastJobid {
		buf.WriteString(lastEvt.getHeader() + "\n")
	}
	if lastJmeta.Finished {
		if lastJmeta.ErrMsg == "" {
			buf.WriteString(lastEvt.getReport() + "\n")
		} else {
			buf.WriteString(redColorText(lastJmeta.ErrMsg) + "\n")
		}
		mgr.events = make([]projEvent, 0)
		mgr.alignIdx = len(mgr.pmeta.JobMetas)
	} else {
		if isTerminal() && lastEvt.showProgress() {
			buf.WriteString(lastEvt.getProgress())
		}
		mgr.events = []projEvent{lastEvt}
		mgr.alignIdx = len(mgr.pmeta.JobMetas) - 1
	}
	mgr.lastJobid = lastEvt.getMeta().JobId
	return buf.String(), mgr.pmeta.Finished
}

func (mgr *progressMgr) clearLastLine() {
	if !isTerminal() || mgr.lastLine == "" {
		return
	}
	buf := make([]byte, 80)
	for i := 0; i < len(buf); i++ {
		buf[i] = ' '
	}
	buf[0] = '\r'
	buf[len(buf)-1] = '\r'
	os.Stdout.Write(buf)
}

func (mgr *progressMgr) getLastLine(msg string) string {
	i := len(msg) - 1
	for ; i >= 0; i-- {
		if msg[i] == '\n' {
			break
		}
	}
	if i == len(msg)-1 {
		return ""
	} else {
		return msg[i+1:]
	}
}

func (mgr *progressMgr) cost() string {
	pmeta := mgr.pmeta
	seconds := int(pmeta.EndTs.Sub(pmeta.StartTs) / time.Second)
	return formatSeconds(seconds)
}

func (mgr *progressMgr) run() {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(SHOW_STATUS_INTERVAL)
		for _ = range ticker.C {
			finished := mgr.show()
			if finished {
				break
			}
		}
	}()
	ticker := time.NewTicker(MONITOR_STATUS_INTERVAL)
	for _ = range ticker.C {
		if err := mgr.fetch(); err != nil {
			panic(err)
		}
		if err := mgr.update(); err != nil {
			panic(err)
		}
		if mgr.pmeta.Finished {
			break
		}
	}
	wg.Wait()
	if mgr.pmeta != nil {
		msg := boldText(fmt.Sprintf("Project done, taken %s.", mgr.cost()))
		fmt.Printf("%s %s\n", blueColorText(formatTs(time.Now())), msg)
	}
}

func saveTo(data string) {
	fp, err := os.OpenFile("/tmp/xx", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	defer fp.Close()
	fp.WriteString(time.Now().Format("15:04:05") + "\n")
	fp.WriteString(data)
}
