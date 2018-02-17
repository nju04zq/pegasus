package lianjia

import (
	"fmt"
	"pegasus/log"
	"pegasus/task"

	"github.com/anaskhan96/soup"
)

const (
	JOB_KIND_DISTRICTS = "Lianjia Crawler: Get districts"
)

type District struct {
	Name string
	Uri  string
}

type JobDistricts struct {
	districts []*District
	nextJobs  []*JobRegions
}

func (job *JobDistricts) AppendInput(input interface{}) {
	return
}

func (job *JobDistricts) Init() error {
	job.districts = make([]*District, 0)
	link := ERSHOUFANG_LINK
	resp, err := soup.Get(link)
	if err != nil {
		return fmt.Errorf("Fail to get from %q, %v", link, err)
	}
	doc := soup.HTMLParse(resp)
	tags := doc.FindAll("div", "data-role", "ershoufang")
	if len(tags) == 0 {
		return fmt.Errorf("No districts detected!")
	} else if len(tags) > 1 {
		return fmt.Errorf("Too many <div data-role> found!")
	}
	tags = tags[0].FindAll("a")
	if len(tags) == 0 {
		return fmt.Errorf("No districts detected!")
	}
	for _, tag := range tags {
		d := &District{
			Name: tag.Text(),
			Uri:  tag.Attrs()["href"],
		}
		log.Info("Get district %q, %q", d.Name, d.Uri)
		job.districts = append(job.districts, d)
	}
	return nil
}

func (job *JobDistricts) GetKind() string {
	return JOB_KIND_DISTRICTS
}

func (job *JobDistricts) CalcTaskCnt() int {
	return 0
}

func (job *JobDistricts) GetNextTask(tid string) *task.TaskSpec {
	return nil
}

func (job *JobDistricts) ReduceTasks(reports []*task.TaskReport) error {
	return nil
}

func (job *JobDistricts) GetOutput() interface{} {
	return job.districts
}

func (job *JobDistricts) GetNextJobs() []task.Job {
	jobs := make([]task.Job, 0, len(job.nextJobs))
	for _, j := range job.nextJobs {
		jobs = append(jobs, j)
	}
	return jobs
}

func (job *JobDistricts) GetTaskGen() task.TaskGenerator {
	return nil
}
