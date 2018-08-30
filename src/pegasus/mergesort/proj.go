package mergesort

import (
	"pegasus/task"
)

const (
	PROJ_MERGESORT = "Mergesort"
)

type ProjMergesort struct {
	err  error
	jobs []task.Job
}

func (proj *ProjMergesort) Init(config string) error {
	proj.InitJobs()
	return nil
}

func (proj *ProjMergesort) InitJobs() {
	j0 := new(JobRandInts)
	j1 := new(JobMergesort)
	j2 := new(JobDumpres)
	j0.nextJobs = []*JobMergesort{j1}
	j1.nextJobs = []*JobDumpres{j2}
	proj.jobs = []task.Job{j0, j1, j2}
}

func (proj *ProjMergesort) GetName() string {
	return PROJ_MERGESORT
}

func (proj *ProjMergesort) GetJobs() []task.Job {
	return proj.jobs
}

func (proj *ProjMergesort) GetEnv() interface{} {
	return nil
}

func (proj *ProjMergesort) SetErr(err error) {
	proj.err = err
}

func (proj *ProjMergesort) GetErr() error {
	return proj.err
}

func (proj *ProjMergesort) Finish(stats *task.ProjStats) error {
	return nil
}
