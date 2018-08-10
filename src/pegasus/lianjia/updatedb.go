package lianjia

import (
	"fmt"
	"pegasus/log"
	"pegasus/task"
	"reflect"
	"strings"

	"github.com/go-gorp/gorp"
)

const (
	JOB_KIND_UPDATE_DB  = "Lianjia Crawler: Update database"
	TASK_KIND_UPDATE_DB = JOB_KIND_UPDATE_DB
)

type JobUpdateDb struct {
	apartments map[string][]*Apartment
	regions    []string
	nextRegion int
}

func (job *JobUpdateDb) AppendInput(input interface{}) {
	if job.apartments == nil {
		job.apartments = make(map[string][]*Apartment)
	}
	apartmentsInput := input.(map[string][]*Apartment)
	for regionAbbr, regionApartments := range apartmentsInput {
		if _, ok := job.apartments[regionAbbr]; !ok {
			job.apartments[regionAbbr] = make([]*Apartment, 0, len(regionApartments))
		}
		a, _ := job.apartments[regionAbbr]
		job.apartments[regionAbbr] = append(a, regionApartments...)
	}
}

func (job *JobUpdateDb) Init(env interface{}) error {
	job.regions = make([]string, 0)
	for regionAbbr, _ := range job.apartments {
		job.regions = append(job.regions, regionAbbr)
	}
	job.nextRegion = 0
	return nil
}

func (job *JobUpdateDb) GetKind() string {
	return JOB_KIND_UPDATE_DB
}

func (job *JobUpdateDb) CalcTaskCnt() int {
	return len(job.regions)
}

func (job *JobUpdateDb) GetNextTask(tid string) *task.TaskSpec {
	if job.nextRegion == len(job.regions) {
		return nil
	}
	nextRegion := job.regions[job.nextRegion]
	spec := &TspecUpdateDb{
		Region:     nextRegion,
		Apartments: job.apartments[nextRegion],
	}
	job.nextRegion++
	return &task.TaskSpec{
		Tid:  tid,
		Kind: TASK_KIND_UPDATE_DB,
		Spec: spec,
	}
}

func (job *JobUpdateDb) ReduceTasks(reports []*task.TaskReport) error {
	return nil
}

func (job *JobUpdateDb) GetOutput() interface{} {
	return nil
}

func (job *JobUpdateDb) GetNextJobs() []task.Job {
	return nil
}

func (job *JobUpdateDb) GetTaskGen() task.TaskGenerator {
	return TaskGenUpdateDb
}

func (job *JobUpdateDb) GetReport() string {
	cnt := 0
	for _, a := range job.apartments {
		cnt += len(a)
	}
	return fmt.Sprintf("Update %d apartments.", cnt)
}

type TspecUpdateDb struct {
	Region     string
	Apartments []*Apartment
}

func TaskGenUpdateDb(tspec *task.TaskSpec) (task.Task, error) {
	tsk := new(taskUpdateDb)
	tsk.tid = tspec.Tid
	tsk.kind = tspec.Kind
	spec := new(TspecUpdateDb)
	task.DecodeSpec(tspec, spec)
	tsk.region = spec.Region
	tsk.apartments = spec.Apartments
	tsk.desc = fmt.Sprintf("Update db for %s", tsk.region)
	return tsk, nil
}

type taskUpdateDb struct {
	err        error
	tid        string
	kind       string
	desc       string
	region     string
	apartments []*Apartment
	done       bool
}

func (tsk *taskUpdateDb) Init(executorCnt int) error {
	tsk.done = false
	return nil
}

func (tsk *taskUpdateDb) NewTaskletCtx() task.TaskletCtx {
	return nil
}

func (tsk *taskUpdateDb) GetTaskId() string {
	return tsk.tid
}

func (tsk *taskUpdateDb) GetKind() string {
	return tsk.kind
}

func (tsk *taskUpdateDb) GetDesc() string {
	return tsk.desc
}

func (tsk *taskUpdateDb) GetTaskletCnt() int {
	return 1
}

func (tsk *taskUpdateDb) GetNextTasklet(taskletid string) task.Tasklet {
	if tsk.done {
		return nil
	}
	t := &taskletUpdateDb{
		tid:        taskletid,
		region:     tsk.region,
		apartments: tsk.apartments,
	}
	tsk.done = true
	return t
}

func (tsk *taskUpdateDb) ReduceTasklets(tasklets []task.Tasklet) {
	return
}

func (tsk *taskUpdateDb) SetError(err error) {
	tsk.err = err
}

func (tsk *taskUpdateDb) GetError() error {
	return tsk.err
}

func (tsk *taskUpdateDb) GetOutput() interface{} {
	return nil
}

type taskletUpdateDb struct {
	tid               string
	region            string
	apartments        []*Apartment
	dbmap             *gorp.DbMap
	dataTblName       string
	dataChangeTblName string
	metaChangeTblName string
}

func (t *taskletUpdateDb) GetTaskletId() string {
	return t.tid
}

func (t *taskletUpdateDb) Execute(ctx task.TaskletCtx) error {
	dbmap, err := getDbmap()
	if err != nil {
		return fmt.Errorf("Fail to get dbmap, %v", err)
	}
	t.dbmap = dbmap
	defer putDbmap()
	if err := t.addTables(); err != nil {
		return fmt.Errorf("Fail to add tables, %v", err)
	}
	if err := t.updateApartments(); err != nil {
		return fmt.Errorf("Fail to update, %v", err)
	}
	return nil
}

func (t *taskletUpdateDb) addTables() error {
	t.dataTblName = t.region + "_data"
	t.dataChangeTblName = t.region + "_change"
	t.metaChangeTblName = t.region + "_change_meta"
	t.dbmap.AddTableWithName(Apartment{}, t.dataTblName).SetKeys(false, "aid")
	t.dbmap.AddTableWithName(ApartmentDataChange{}, t.dataChangeTblName)
	t.dbmap.AddTableWithName(ApartmentMetaChange{}, t.metaChangeTblName)
	if err := t.dbmap.CreateTablesIfNotExists(); err != nil {
		return err
	}
	return nil
}

func (t *taskletUpdateDb) updateApartments() (err error) {
	objbuf := new(objBuf).init(t.dbmap)
	oldApartments, err := t.getOldApartments()
	if err != nil {
		return err
	}
	for _, apartment := range t.apartments {
		old, ok := oldApartments[apartment.Aid]
		if !ok {
			err = t.insertOneApartment(objbuf, apartment)
		} else {
			err = t.updateOneApartment(objbuf, old, apartment)
		}
		if err != nil {
			log.Error("Fail to update apartments, %v", err)
			return err
		}
	}
	if err = objbuf.flush(); err != nil {
		log.Error("Fail to flush apartments, %v", err)
		return err
	}
	return nil
}

func (t *taskletUpdateDb) getOldApartments() (map[string]*Apartment, error) {
	var apartments []*Apartment
	query := fmt.Sprintf("SELECT * FROM %s", t.dataTblName)
	_, err := t.dbmap.Select(&apartments, query)
	if err != nil {
		log.Error("Fail to get apartments, %v", err)
		return nil, err
	}
	dict := make(map[string]*Apartment)
	for _, apartment := range apartments {
		if _, ok := dict[apartment.Aid]; ok {
			log.Error("Duplicate apartment %s found", apartment.Aid)
		} else {
			dict[apartment.Aid] = apartment
		}
	}
	return dict, nil
}

func (t *taskletUpdateDb) insertOneApartment(objbuf *objBuf, apartment *Apartment) error {
	return objbuf.insert(apartment)
}

func (t *taskletUpdateDb) updateOneApartment(objbuf *objBuf, old, new *Apartment) (err error) {
	new.Nts = old.Nts
	if err := objbuf.update(new); err != nil {
		return err
	}
	err = t.updateDataChange(objbuf, old, new)
	if err != nil {
		return err
	}
	err = t.updateMetaChange(objbuf, old, new)
	if err != nil {
		return err
	}
	return nil
}

func (t *taskletUpdateDb) updateDataChange(objbuf *objBuf, old, new *Apartment) (err error) {
	if old.Price == new.Price && old.Total == new.Total {
		return nil
	}
	change := &ApartmentDataChange{
		Aid:      old.Aid,
		OldPrice: old.Price,
		NewPrice: new.Price,
		OldTotal: old.Total,
		NewTotal: new.Total,
		Ts:       new.Uts,
	}
	if err = objbuf.insert(change); err != nil {
		return err
	}
	return nil
}

func (t *taskletUpdateDb) updateMetaChange(objbuf *objBuf, old, new *Apartment) (err error) {
	blacklist := map[string]bool{
		"Visitcnt": true,
	}
	isInt := func(kind reflect.Kind) bool {
		if kind == reflect.Int { // TODO for int8...
			return true
		} else {
			return false
		}
	}
	val2str := func(v reflect.Value) string {
		return fmt.Sprintf("%v", v.Interface())
	}
	vOld, vNew := reflect.ValueOf(old).Elem(), reflect.ValueOf(new).Elem()
	atype := vOld.Type()
	for i := 0; i < atype.NumField(); i++ {
		field := atype.Field(i)
		if _, ok := blacklist[field.Name]; ok {
			continue
		}
		fname := strings.ToLower(field.Name)
		fOld, fNew := vOld.Field(i), vNew.Field(i)
		fKind := field.Type.Kind()
		theSame := true
		if isInt(fKind) {
			if fOld.Int() != fNew.Int() {
				theSame = false
			}
		} else if fKind == reflect.String {
			if fOld.String() != fNew.String() {
				theSame = false
			}
		}
		if theSame {
			continue
		}
		s0, s1 := val2str(fOld), val2str(fNew)
		err := t.updateOneMetaChange(objbuf, old.Aid, fname, s0, s1, new.Uts)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *taskletUpdateDb) updateOneMetaChange(objbuf *objBuf, aid, item, old, new string, ts int64) (err error) {
	change := &ApartmentMetaChange{
		Aid:  aid,
		Item: item,
		Old:  old,
		New:  new,
		Ts:   ts,
	}
	return objbuf.insert(change)
}

const (
	OBJ_BUF_SIZE = 4
)

type objBuf struct {
	nextInsert int
	nextUpdate int
	objsInsert []interface{}
	objsUpdate []interface{}
	dbmap      *gorp.DbMap
	method     int
}

func (buf *objBuf) init(dbmap *gorp.DbMap) *objBuf {
	buf.nextInsert = 0
	buf.nextUpdate = 0
	buf.objsInsert = make([]interface{}, OBJ_BUF_SIZE)
	buf.objsUpdate = make([]interface{}, OBJ_BUF_SIZE)
	buf.dbmap = dbmap
	return buf
}

func (buf *objBuf) insert(obj interface{}) (err error) {
	buf.objsInsert[buf.nextInsert] = obj
	buf.nextInsert++
	if buf.nextInsert == OBJ_BUF_SIZE {
		err = buf.flush()
	}
	return err
}

func (buf *objBuf) update(obj interface{}) (err error) {
	buf.objsUpdate[buf.nextUpdate] = obj
	buf.nextUpdate++
	if buf.nextUpdate == OBJ_BUF_SIZE {
		err = buf.flush()
	}
	return err
}

func (buf *objBuf) flush() (err error) {
	var objs []interface{}
	if buf.nextInsert > 0 {
		objs = buf.objsInsert[:buf.nextInsert]
		err = buf.dbmap.Insert(objs...)
		buf.nextInsert = 0
	}
	if buf.nextUpdate > 0 {
		objs = buf.objsUpdate[:buf.nextUpdate]
		_, err = buf.dbmap.Update(objs...)
		buf.nextUpdate = 0
	}
	if err != nil {
		log.Error("Fail to insert/update %v, %v", objs[0], err)
	}
	return err
}

/*
var testApartments = []*Apartment{
	&Apartment{
		Location: "local1",
		Aid:      "1",
		Price:    5,
		Size:     "size3",
		Total:    14,
		Nts:      time.Now().Unix(),
		Uts:      time.Now().Unix(),
		Subway:   1,
		Station:  "station3",
		Smeter:   102,
		Floor:    "L",
		Tfloor:   10,
		Year:     1001,
		Withlift: "U",
		Visitcnt: 105,
	},
	&Apartment{
		Location: "local3",
		Aid:      "2",
		Price:    3,
		Size:     "size4",
		Total:    11,
		Nts:      time.Now().Unix(),
		Uts:      time.Now().Unix(),
		Subway:   2,
		Station:  "station3",
		Smeter:   101,
		Floor:    "H",
		Tfloor:   11,
		Year:     1001,
		Withlift: "Y",
		Visitcnt: 121,
	},
}

func initLogger() error {
	consoleLogger := &log.ConsoleLogger{
		//Level: log.LevelInfo,
		Level: log.LevelDebug,
	}
	if err := log.RegisterLogger(consoleLogger); err != nil {
		return err
	}
	log.Info("=====Start Master server=====")
	return nil
}

func RunTasklet() {
	initLogger()
	tasklet := &taskletUpdateDb{
		region:     "test",
		apartments: testApartments,
	}
	if err := tasklet.Execute(nil); err != nil {
		panic(err)
	}
}
*/
