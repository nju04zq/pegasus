package lianjia

import (
	"bytes"
	"fmt"
	"pegasus/log"
	"pegasus/rate"
	"pegasus/task"
	"pegasus/util"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/anaskhan96/soup"
)

const (
	JOB_KIND_GET_APARTMENTS  = "Lianjia Crawler: Get apartments"
	TASK_KIND_GET_APARTMENTS = JOB_KIND_GET_APARTMENTS
)

type Apartment struct {
	Location string `db:"location,size:64"`
	Aid      string `db:"aid,size:32"`
	Price    int    `db:"price"`
	Size     string `db:"size,size:32"`
	Total    int    `db:"total"`
	Nts      int64  `db:"nts"`
	Uts      int64  `db:"uts"`
	Subway   int    `db:"subway"`
	Station  string `db:"station,size:16"`
	Smeter   int    `db:"smeter"`
	Floor    string `db:"floor,size:4"`
	Tfloor   int    `db:"tfloor"`
	Year     int    `db:"year"`
	Withlift string `db:"withlift,size:4"`
	Visitcnt int    `db:"visitcnt"`
}

type ApartmentDataChange struct {
	Aid      string `db:"aid,size:32"`
	OldPrice int    `db:"old_price"`
	NewPrice int    `db:"new_price"`
	OldTotal int    `db:"old_total"`
	NewTotal int    `db:"new_total"`
	Ts       int64  `db:"ts"`
}

type ApartmentMetaChange struct {
	Aid  string `db:"aid,size:32"`
	Item string `db:"item,size:16"`
	Old  string `db:"old,size:64"`
	New  string `db:"new,size:64"`
	Ts   int64  `db:"ts"`
}

func (a *Apartment) String() string {
	buf := bytes.NewBuffer(nil)
	buf.WriteString("======================================\n")
	buf.WriteString(fmt.Sprintf("%s %s\n", a.Aid, a.Location))
	buf.WriteString(fmt.Sprintf("%d %s %d\n", a.Price, a.Size, a.Total))
	buf.WriteString(fmt.Sprintf("%d %s %d\n", a.Subway, a.Station, a.Smeter))
	buf.WriteString(fmt.Sprintf("%s %d %d\n", a.Floor, a.Tfloor, a.Year))
	buf.WriteString(fmt.Sprintf("%s %d\n", a.Withlift, a.Visitcnt))
	return buf.String()
}

type RegionApartments struct {
	RegionAbbr string
	Apartments []*Apartment
}

type JobGetApartments struct {
	regions    []*Region
	taskSize   int
	nextRegion int
	apartments map[string][]*Apartment
	nextJobs   []*JobUpdateDb
}

func (job *JobGetApartments) AppendInput(input interface{}) {
	a := input.([]*Region)
	if job.regions == nil {
		job.regions = make([]*Region, 0)
	}
	for _, region := range a {
		job.regions = append(job.regions, region)
	}
}

func (job *JobGetApartments) Init(env interface{}) error {
	for _, region := range job.regions {
		if region.MaxPage > 0 {
			job.taskSize++
		}
	}
	job.apartments = make(map[string][]*Apartment)
	return nil
}

func (job *JobGetApartments) GetKind() string {
	return JOB_KIND_GET_APARTMENTS
}

func (job *JobGetApartments) CalcTaskCnt() int {
	return job.taskSize
}

func (job *JobGetApartments) GetNextTask(tid string) *task.TaskSpec {
	for {
		if job.nextRegion >= job.taskSize {
			return nil
		}
		if job.regions[job.nextRegion].MaxPage > 0 {
			break
		}
		job.nextRegion++
	}
	region := job.regions[job.nextRegion]
	spec := &TspecGetApartments{
		Desc:       fmt.Sprintf("%s(%s)", region.Name, region.Dists[0].Name),
		RegionInfo: region,
	}
	job.nextRegion++
	return &task.TaskSpec{
		Tid:  tid,
		Kind: TASK_KIND_GET_APARTMENTS,
		Spec: spec,
	}
}

func (job *JobGetApartments) ReduceTasks(reports []*task.TaskReport) error {
	for _, report := range reports {
		apartments := new(RegionApartments)
		if err := util.FitDataInto(report.Output, &apartments); err != nil {
			return err
		}
		job.apartments[apartments.RegionAbbr] = apartments.Apartments
	}
	return nil
}

func (job *JobGetApartments) GetOutput() interface{} {
	return job.apartments
}

func (job *JobGetApartments) GetNextJobs() []task.Job {
	jobs := make([]task.Job, 0, len(job.nextJobs))
	for _, j := range job.nextJobs {
		jobs = append(jobs, j)
	}
	return jobs
}

func (job *JobGetApartments) GetTaskGen() task.TaskGenerator {
	return TaskGenGetApartments
}

func (job *JobGetApartments) GetReport() string {
	cnt := 0
	for _, a := range job.apartments {
		cnt += len(a)
	}
	return fmt.Sprintf("Get %d apartments.", cnt)
}

type TspecGetApartments struct {
	Desc       string
	RegionInfo *Region
}

func TaskGenGetApartments(tspec *task.TaskSpec) (task.Task, error) {
	tsk := new(taskGetApartments)
	tsk.tid = tspec.Tid
	tsk.kind = tspec.Kind
	spec := new(TspecGetApartments)
	task.DecodeSpec(tspec, spec)
	tsk.region = spec.RegionInfo
	tsk.desc = spec.Desc
	return tsk, nil
}

type taskGetApartments struct {
	err        error
	tid        string
	kind       string
	desc       string
	region     *Region
	curPage    int
	maxPage    int
	apartments []*Apartment
}

func (tsk *taskGetApartments) Init(executorCnt int) error {
	tsk.curPage = 1
	tsk.maxPage = tsk.region.MaxPage
	return nil
}

func (tsk *taskGetApartments) NewTaskletCtx() task.TaskletCtx {
	return nil
}

func (tsk *taskGetApartments) GetTaskId() string {
	return tsk.tid
}

func (tsk *taskGetApartments) GetKind() string {
	return tsk.kind
}

func (tsk *taskGetApartments) GetDesc() string {
	return tsk.desc
}

func (tsk *taskGetApartments) GetTaskletCnt() int {
	return tsk.maxPage
}

func (tsk *taskGetApartments) GetNextTasklet(taskletid string) task.Tasklet {
	if tsk.curPage > tsk.maxPage {
		return nil
	}
	t := &taskletGetApartments{
		tid:    taskletid,
		region: tsk.region,
		page:   tsk.curPage,
	}
	tsk.curPage++
	return t
}

func (tsk *taskGetApartments) ReduceTasklets(tasklets []task.Tasklet) {
	set := make(map[string]bool)
	for _, t := range tasklets {
		tasklet := t.(*taskletGetApartments)
		for _, apartment := range tasklet.apartments {
			if _, ok := set[apartment.Aid]; ok {
				log.Error("Drop duplicate apartment %s", apartment.Aid)
				continue
			}
			set[apartment.Aid] = true
			tsk.apartments = append(tsk.apartments, apartment)
		}
	}
}

func (tsk *taskGetApartments) SetError(err error) {
	tsk.err = err
}

func (tsk *taskGetApartments) GetError() error {
	return tsk.err
}

func (tsk *taskGetApartments) GetOutput() interface{} {
	return &RegionApartments{
		RegionAbbr: tsk.region.Abbr,
		Apartments: tsk.apartments,
	}
}

type taskletGetApartments struct {
	tid        string
	region     *Region
	page       int
	apartments []*Apartment
}

func (t *taskletGetApartments) GetTaskletId() string {
	return t.tid
}

func (t *taskletGetApartments) Execute(ctx task.TaskletCtx) error {
	link := regionPgLink(t.region, t.page)
	log.Info("Get apartments from %q", link)
	resp, err := rate.GetHtml(link)
	if err != nil {
		return fmt.Errorf("Fail to get apartments from %q, %v", link, err)
	}
	doc := soup.HTMLParse(resp)
	tags, err := findAll(&doc, 0, 0, "div", "class", "info clear")
	if err != nil {
		return fmt.Errorf("Fail to get apartment list")
	}
	t.apartments = make([]*Apartment, 0, len(tags))
	for _, tag := range tags {
		apartment, err := t.parseApartment(&tag)
		if err != nil {
			log.Error("Fail to parse apartment in %s, %s, %v", link, render(&tag), err)
			continue
		}
		t.apartments = append(t.apartments, apartment)
	}
	return nil
}

func (t *taskletGetApartments) parseApartment(root *soup.Root) (*Apartment, error) {
	// <a "data-el"="ershoufang">, for aid
	tags, err := findAll(root, 1, 1, "a", "data-el", "ershoufang")
	if err != nil {
		tags, err = findAll(root, 1, 1, "a", "class", "LOGCLICKDATA ")
		if err != nil {
			return nil, err
		}
	}
	href, err := tagAttr(&tags[0], "href")
	if err != nil {
		return nil, err
	}
	aid, err := getAidFromHref(href)
	if err != nil {
		return nil, err
	}
	// <a "data-el"="region">, for location
	tags, err = findAll(root, 1, 1, "a", "data-el", "region")
	if err != nil {
		return nil, err
	}
	location := tags[0].Text()
	location = t.stripLocation(location)
	// <div "class"="houseInfo">, for size, withLift
	tags, err = findAll(root, 1, 1, "div", "class", "houseInfo")
	if err != nil {
		return nil, err
	}
	s := render(&tags[0])
	re := regexp.MustCompile(`\|[ ]* ([0-9.]+)平`)
	res := re.FindStringSubmatch(s)
	if len(res) == 0 {
		log.Error("Apartment size not found in %s", s)
		return nil, fmt.Errorf("Apartment size not found")
	}
	size := res[1]
	withLift := "U"
	if strings.Contains(s, "有电梯") {
		withLift = "Y"
	} else if strings.Contains(s, "无电梯") {
		withLift = "N"
	}
	// <div class="unitPrice">, for price
	tags, err = findAll(root, 1, 1, "div", "class", "unitPrice")
	if err != nil {
		return nil, err
	}
	s = render(&tags[0])
	re = regexp.MustCompile(`单价(\d+)元/平米`)
	res = re.FindStringSubmatch(s)
	if len(res) == 0 {
		log.Error("Apartment size not found in %s", s)
		return nil, fmt.Errorf("Price info not found")
	}
	price64, err := strconv.ParseInt(res[1], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("Price %q not int", res[1])
	}
	price := int(price64)
	// <div class="totalPrice">, for total
	tags, err = findAll(root, 1, 1, "div", "class", "totalPrice")
	if err != nil {
		return nil, err
	}
	re = regexp.MustCompile(`>(\d+[.]?\d+)<`)
	res = re.FindStringSubmatch(render(&tags[0]))
	if len(res) == 0 {
		return nil, fmt.Errorf("Apartment size not found")
	}
	total64, err := strconv.ParseFloat(res[1], 32)
	if err != nil {
		return nil, fmt.Errorf("Price %q not float", res[1])
	}
	total := int(total64)
	// <div class="positionInfo">, for floor, tfloor, year
	tags, err = findAll(root, 1, 1, "div", "class", "positionInfo")
	if err != nil {
		return nil, err
	}
	floor, tfloor, year, err := t.parsePosition(&tags[0])
	if err != nil {
		return nil, err
	}
	// <span class="subway">, for subway, station, smeter
	tags, err = findAll(root, 0, 1, "span", "class", "subway")
	if err != nil {
		return nil, err
	}
	subway, station, smeter, err := t.parseSubway(tags)
	if err != nil {
		return nil, err
	}
	// <div class="followInfo">, for visitcnt
	tags, err = findAll(root, 1, 1, "div", "class", "followInfo")
	if err != nil {
		return nil, err
	}
	/*
		 //带看信息页面中不再提供 2019/07/05出现, 2019/07/25修复
			re = regexp.MustCompile(`共(\d+)次带看`)
			res = re.FindStringSubmatch(render(&tags[0]))
			if len(res) == 0 {
				return nil, fmt.Errorf("Follow info not found")
			}
			visitcnt64, _ := strconv.ParseInt(res[1], 10, 32)
			visitcnt := int(visitcnt64)
	*/
	return &Apartment{
		Aid:      aid,
		Location: location,
		Price:    price,
		Size:     size,
		Total:    total,
		Subway:   subway,
		Station:  station,
		Smeter:   smeter,
		Floor:    floor,
		Tfloor:   tfloor,
		Year:     year,
		Withlift: withLift,
		Nts:      time.Now().Unix(),
		Uts:      time.Now().Unix(),
	}, nil
}

func (t *taskletGetApartments) stripLocation(location string) string {
	location = strings.Replace(location, " ", "", -1)
	return location
}

func (t *taskletGetApartments) parsePosition(tag *soup.Root) (string, int, int, error) {
	floor, tfloor, year := "U", 0, 0
	re := regexp.MustCompile(`span>(.*)楼层.*共(\d+)层.*(\d{4})年建`)
	res := re.FindStringSubmatch(render(tag))
	if len(res) == 0 {
		return floor, tfloor, year, nil
	}
	switch res[1] {
	case "低":
		floor = "L"
	case "中":
		floor = "M"
	case "高":
		floor = "H"
	}
	tfloor64, err := strconv.ParseInt(res[2], 10, 32)
	if err != nil {
		return floor, tfloor, year, fmt.Errorf("Floor %q not int", res[2])
	}
	year64, err := strconv.ParseInt(res[3], 10, 32)
	if err != nil {
		return floor, tfloor, year, fmt.Errorf("Year %q not int", res[3])
	}
	tfloor, year = int(tfloor64), int(year64)
	return floor, tfloor, year, nil
}

func (t *taskletGetApartments) parseSubway(tags []soup.Root) (int, string, int, error) {
	subway, station, smeter := 0, "", 0
	if len(tags) == 0 {
		return subway, station, smeter, nil
	}
	re := regexp.MustCompile(`距离(\d+)号线(.*)站(\d+)米`)
	res := re.FindStringSubmatch(render(&tags[0]))
	if len(res) == 0 {
		return subway, station, smeter, nil
	}
	subway64, _ := strconv.ParseInt(res[1], 10, 32)
	smeter64, _ := strconv.ParseInt(res[3], 10, 32)
	subway, smeter = int(subway64), int(smeter64)
	station = res[2]
	return subway, station, smeter, nil
}
