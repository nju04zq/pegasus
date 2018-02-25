package lianjia

import (
	"fmt"
	"pegasus/log"
	"pegasus/rate"
	"pegasus/task"

	"github.com/anaskhan96/soup"
)

const (
	JOB_KIND_DISTRICTS = "Lianjia Crawler: Get districts"
)

type District struct {
	Name string
	Abbr string
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
	resp, err := rate.GetHtml(link)
	if err != nil {
		return fmt.Errorf("Fail to get from %q, %v", link, err)
	}
	doc := soup.HTMLParse(resp)
	tags, err := findAll(&doc, 1, 1, "div", "data-role", "ershoufang")
	if err != nil {
		return err
	}
	tags, err = findAll(&tags[0], 1, -1, "a")
	if err != nil {
		return err
	}
	for _, tag := range tags {
		uri, err := tagAttr(&tag, "href")
		if err != nil {
			return err
		}
		abbr, err := parseAbbr(uri)
		if err != nil {
			return err
		}
		d := &District{
			Name: tag.Text(),
			Abbr: abbr,
		}
		log.Info("Get district %q, %q", d.Name, d.Abbr)
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
