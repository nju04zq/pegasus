package lianjia

import "pegasus/task"

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
	j0.nextJobs = []*JobRegions{j1}
	proj.jobs = []task.Job{j0, j1}
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
	return nil
}
