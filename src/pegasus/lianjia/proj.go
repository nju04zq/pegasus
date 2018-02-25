package lianjia

import (
	"pegasus/log"
	"pegasus/rate"
	"pegasus/task"
)

const (
	PROJ_LIANJIA = "Lianjia-Crawler"
)

type ProjLianjia struct {
	err  error
	jobs []task.Job
}

func (proj *ProjLianjia) Init() error {
	j0 := new(JobDistricts)
	j1 := new(JobRegions)
	j2 := new(JobRegionMaxpage)
	j3 := new(JobGetApartments)
	j0.nextJobs = []*JobRegions{j1}
	j1.nextJobs = []*JobRegionMaxpage{j2}
	j2.nextJobs = []*JobGetApartments{j3}
	proj.jobs = []task.Job{j0, j1, j2, j3}
	return nil
}

func (proj *ProjLianjia) GetName() string {
	return PROJ_LIANJIA
}

func (proj *ProjLianjia) GetJobs() []task.Job {
	return proj.jobs
}

func (proj *ProjLianjia) SetErr(err error) {
	proj.err = err
}

func (proj *ProjLianjia) GetErr() error {
	return proj.err
}

func (proj *ProjLianjia) Finish() error {
	log.Info(rate.Summary())
	return nil
}
