package mergesort

import (
	"pegasus/task"
	"time"
)

type ProjMergesort struct {
	err     error
	name    string
	startTs time.Time
	endTs   time.Time
	jobs    []task.Job
}

func (proj *ProjMergesort) Init() error {
	proj.name = "Mergesort"
	proj.startTs = time.Now()
	j0 := new(JobRandInts)
	j1 := new(JobMergesort)
	j2 := new(JobDumpres)
	j0.nextJobs = []*JobMergesort{j1}
	j1.nextJobs = []*JobDumpres{j2}
	proj.jobs = []task.Job{j0, j1, j2}
	return nil
}

func (proj *ProjMergesort) GetName() string {
	return proj.name
}

func (proj *ProjMergesort) GetJobs() []task.Job {
	return proj.jobs
}

func (proj *ProjMergesort) GetStartTs() time.Time {
	return proj.startTs
}

func (proj *ProjMergesort) GetEndTs() time.Time {
	return proj.endTs
}

func (proj *ProjMergesort) SetErr(err error) {
	proj.err = err
}

func (proj *ProjMergesort) GetErr() error {
	return proj.err
}

func (proj *ProjMergesort) Finish() error {
	proj.endTs = time.Now()
	return nil
}
