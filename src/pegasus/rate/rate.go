package rate

import (
	"fmt"
	"net/http"
	"pegasus/log"
	"pegasus/route"
	"pegasus/server"
	"pegasus/util"
	"sync"
	"time"

	"github.com/anaskhan96/soup"
)

var masterinfo = new(masterInfo)

type masterInfo struct {
	ip   string
	port int
}

const (
	masterRateUri       = "/rate"
	masterWorkerRateUri = "/worker/rate"

	workerReportInterval = 1 * time.Second
)

var rateStats = new(RateStats)

type RateStats struct {
	mutex         sync.Mutex
	TotalBytes    int
	TotalDuration time.Duration
	SuccessCnt    int
	FailureCnt    int
	MaxDuration   time.Duration
	MinDuration   time.Duration
}

func (stats *RateStats) update(bytes int, duration time.Duration) {
	stats.mutex.Lock()
	defer stats.mutex.Unlock()
	stats.TotalBytes += bytes
	stats.TotalDuration += duration
	stats.SuccessCnt++
	if stats.MaxDuration == 0 || stats.MaxDuration < duration {
		stats.MaxDuration = duration
	}
	if stats.MinDuration == 0 || stats.MinDuration > duration {
		stats.MinDuration = duration
	}
}

func (stats *RateStats) recordFailure() {
	stats.mutex.Lock()
	defer stats.mutex.Unlock()
	stats.FailureCnt++
}

func (stats *RateStats) snapshot() *RateStats {
	stats.mutex.Lock()
	defer stats.mutex.Unlock()
	return &RateStats{
		TotalBytes:    stats.TotalBytes,
		TotalDuration: stats.TotalDuration,
		SuccessCnt:    stats.SuccessCnt,
		FailureCnt:    stats.FailureCnt,
		MaxDuration:   stats.MaxDuration,
		MinDuration:   stats.MinDuration,
	}
}

func (stats *RateStats) clear() *RateStats {
	stats.mutex.Lock()
	defer stats.mutex.Unlock()
	data := &RateStats{
		TotalBytes:    stats.TotalBytes,
		TotalDuration: stats.TotalDuration,
		SuccessCnt:    stats.SuccessCnt,
		FailureCnt:    stats.FailureCnt,
		MaxDuration:   stats.MaxDuration,
		MinDuration:   stats.MinDuration,
	}
	stats.TotalBytes = 0
	stats.TotalDuration = 0
	stats.SuccessCnt = 0
	stats.FailureCnt = 0
	stats.MaxDuration = 0
	stats.MinDuration = 0
	return data
}

func (stats *RateStats) combine(s *RateStats) {
	stats.mutex.Lock()
	defer stats.mutex.Unlock()
	stats.TotalBytes += s.TotalBytes
	stats.TotalDuration += s.TotalDuration
	stats.SuccessCnt += s.SuccessCnt
	stats.FailureCnt += s.FailureCnt
	if stats.MaxDuration == 0 || stats.MaxDuration < s.MaxDuration {
		stats.MaxDuration = s.MaxDuration
	}
	if stats.MinDuration == 0 || stats.MinDuration > s.MinDuration {
		stats.MinDuration = s.MinDuration
	}
}

func (stats *RateStats) summary() string {
	stats.mutex.Lock()
	defer stats.mutex.Unlock()
	s := fmt.Sprintf("Total %d, %v, success %d, failure %d, max %v, min %v",
		stats.TotalBytes, stats.TotalDuration, stats.SuccessCnt,
		stats.FailureCnt, stats.MaxDuration, stats.MinDuration)
	return s
}

func GetHtml(link string) (string, error) {
	t1 := time.Now()
	resp, err := soup.Get(link)
	t2 := time.Now()
	if err == nil {
		rateStats.update(len(resp), t2.Sub(t1))
	} else {
		rateStats.recordFailure()
	}
	return resp, err
}

func Summary() string {
	return rateStats.summary()
}

func reportRate(args interface{}) {
	stats := rateStats.clear()
	if stats.SuccessCnt+stats.FailureCnt == 0 {
		return
	}
	u := &util.HttpUrl{
		IP:   masterinfo.ip,
		Port: masterinfo.port,
		Uri:  masterWorkerRateUri,
	}
	if _, err := util.HttpPostData(u, stats); err != nil {
		log.Error("Fail to post rate data, %v", err)
		rateStats.combine(stats)
	}
}

func taskRateHandler(w http.ResponseWriter, r *http.Request) {
	stats := new(RateStats)
	if err := util.HttpFitRequestInto(r, stats); err != nil {
		log.Error("Fail to read rate report, %v", err)
		server.FmtResp(w, err, nil)
		return
	}
	rateStats.combine(stats)
	server.FmtResp(w, nil, nil)
}

func masterRateHandler(w http.ResponseWriter, r *http.Request) {
	stats := rateStats.snapshot()
	server.FmtResp(w, nil, stats)
}

func registerRoutes() {
	route.RegisterRoute(&route.Route{
		Name:    "taskRateHandler",
		Method:  http.MethodPost,
		Path:    masterWorkerRateUri,
		Handler: taskRateHandler,
	})
	route.RegisterRoute(&route.Route{
		Name:    "masterRateHandler",
		Method:  http.MethodGet,
		Path:    masterRateUri,
		Handler: masterRateHandler,
	})
}

func InitAsMaster() {
	registerRoutes()
}

func InitAsWorker(masterIp string, masterPort int) {
	masterinfo.ip, masterinfo.port = masterIp, masterPort
	go util.PeriodicalRoutine(true, workerReportInterval, reportRate, nil)
}
