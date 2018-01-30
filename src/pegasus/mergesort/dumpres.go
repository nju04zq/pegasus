package mergesort

import (
	"encoding/json"
	"fmt"
	"os"
	"pegasus/log"
	"pegasus/task"
	"time"
)

const (
	JOB_KIND_DUMPRES = "Mergesort:Dump-result"
)

type JobDumpres struct {
	input      []int
	startTs    time.Time
	endTs      time.Time
	outputFile string
}

func (job *JobDumpres) AppendInput(input interface{}) {
	a := input.([]int)
	job.input = append(job.input, a...)
	return
}

func (job *JobDumpres) Init() error {
	buf, err := json.Marshal(job.input)
	if err != nil {
		return err
	}
	ts := time.Now().UnixNano()
	fname := fmt.Sprintf("/tmp/%d", ts)
	f, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer f.Close()
	f.Write(buf)
	log.Info("Job %q done, result file %q", job.GetKind(), fname)
	return nil
}

func (job *JobDumpres) GetStartTs() time.Time {
	return job.startTs
}

func (job *JobDumpres) GetEndTs() time.Time {
	return job.endTs
}

func (job *JobDumpres) GetKind() string {
	return JOB_KIND_DUMPRES
}

func (job *JobDumpres) CalcTaskCnt() int {
	return 0
}

func (job *JobDumpres) GetNextTask(tid string) *task.TaskSpec {
	return nil
}

func (job *JobDumpres) ReduceTasks(reports []*task.TaskReport) error {
	return nil
}

func (job *JobDumpres) GetOutput() interface{} {
	return job.outputFile
}

func (job *JobDumpres) GetNextJobs() []task.Job {
	return nil
}

func (job *JobDumpres) GetTaskGen() task.TaskGenerator {
	return nil
}
