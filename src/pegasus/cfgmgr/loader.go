package cfgmgr

import (
	"encoding/json"
	"fmt"
	"os"
	"pegasus/log"
	"pegasus/uri"
	"pegasus/util"
	"reflect"
	"strings"
)

const (
	confPathSep = "."
)

type config map[string]interface{}

type configSchemaEntry struct {
	cDef interface{}
}

var conf = config{}

var cfgSchema = make(map[string]*configSchemaEntry)

func mustPtr(cList ...interface{}) {
	for _, c := range cList {
		t := reflect.TypeOf(c)
		if t.Kind() != reflect.Ptr {
			panic(fmt.Errorf("Register with non-ptr, %s", t.Name()))
		}
	}
}

func mustSame(v, vDef reflect.Value) {
	name := v.Type().Name()
	nameDef := vDef.Type().Name()
	if name != nameDef {
		panic(fmt.Errorf("Register cfgEntry with different types, %s, %s",
			name, nameDef))
	}
	path := v.Type().PkgPath()
	pathDef := vDef.Type().PkgPath()
	if path != pathDef {
		panic(fmt.Errorf("Register cfgEntry %s with different path, %s, %s",
			name, path, pathDef))
	}
}

func isSimpleType(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Bool:
		return true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	case reflect.Float32, reflect.Float64:
		return true
	case reflect.String:
		return true
	}
	return false
}

func mustSimpleStruct(v reflect.Value) {
	name := v.Type().Name()
	if v.Kind() != reflect.Struct {
		panic(fmt.Errorf("%s not a struct", name))
	}
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		if !isSimpleType(f) {
			fname := f.Type().Field(i).Name
			panic(fmt.Errorf("%s, field %s not simple type", name, fname))
		}
	}
}

func composeCfgEntryPath(v reflect.Value) string {
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	path := v.Type().PkgPath() + confPathSep + v.Type().Name()
	path = strings.Replace(path, "/", ".", -1)
	return path
}

func RegisterCfgEntry(c interface{}, cDef interface{}) {
	mustPtr(c, cDef)
	v := reflect.ValueOf(c).Elem()
	vDef := reflect.ValueOf(cDef).Elem()
	mustSame(v, vDef)
	mustSimpleStruct(v)
	path := composeCfgEntryPath(v)
	log.Info("Register cfg entry %s", path)
	if _, ok := cfgSchema[path]; ok {
		panic(fmt.Errorf("Cfg entry %s already registered", path))
	}
	entry := &configSchemaEntry{
		cDef: cDef,
	}
	cfgSchema[path] = entry
}

func dumpCfgEntry() {
	for path, c := range conf {
		v := reflect.ValueOf(c).Elem()
		t, k := v.Type(), v.Kind()
		log.Info("path %s, cfg entry %s, kind %v", path, t.Name(), k.String())
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)
			ftype := v.Type().Field(i)
			log.Info("Field %s: %v", ftype.Name, f.Interface())
		}
	}
}

func LoadCfgFromFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDONLY, 0755)
	if err != nil {
		log.Error("Fail to open %s, %v", path, err)
		return err
	}
	defer f.Close()
	c := config{}
	if err := json.NewDecoder(f).Decode(&c); err != nil {
		log.Error("Fail to unmarshal config file %s, %v", path, err)
		return err
	}
	for path, entry := range cfgSchema {
		if _, ok := c[path]; ok {
			log.Info("Load cfg %s from file content", path)
			if cNew, err := loadFromCfg(c[path], entry.cDef); err != nil {
				log.Error("Fail to load %s, %v", path, err)
				return err
			} else {
				conf[path] = cNew
			}
		} else {
			log.Info("Load cfg %s from default value", path)
			conf[path] = loadFromCfgDef(entry.cDef)
		}
	}
	dumpCfgEntry()
	return nil
}

func loadFromCfg(cIn interface{}, cDef interface{}) (interface{}, error) {
	buf, err := json.Marshal(cIn)
	if err != nil {
		log.Error("Fail to marshal %v, %v", cIn, err)
		return nil, err
	}
	v := reflect.New(reflect.ValueOf(cDef).Elem().Type())
	c := v.Interface()
	if err = json.Unmarshal(buf, c); err != nil {
		log.Error("Fail to unmarshal %s from %v, %v", v.Type().Name(), cIn, err)
		return nil, err
	}
	return c, nil
}

func loadFromCfgDef(cDef interface{}) interface{} {
	vsrc := reflect.ValueOf(cDef)
	vdst := reflect.New(vsrc.Elem().Type())
	copySimpleStruct(vsrc, vdst)
	return vdst.Interface()
}

func copySimpleStruct(vsrc, vdst reflect.Value) {
	vsrc, vdst = vsrc.Elem(), vdst.Elem()
	for i := 0; i < vsrc.Type().NumField(); i++ {
		vdst.Field(i).Set(vsrc.Field(i))
	}
}

func SaveCfgToJson(path string) error {
	return saveCfgToJson(path, conf)
}

func SaveDefaultCfgToJson(path string) error {
	c := config{}
	for path, entry := range cfgSchema {
		c[path] = entry.cDef
	}
	return saveCfgToJson(path, c)
}

func saveCfgToJson(path string, c config) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		log.Error("Fail to open %s, %v", path, err)
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(c); err != nil {
		log.Error("Fail to marshal config, %v", err)
		return err
	}
	return nil
}

func GetCfg(path string) (interface{}, error) {
	c, ok := conf[path]
	if !ok {
		return nil, fmt.Errorf("Config path %s not found", path)
	} else {
		return c, nil
	}
}

func PullCfg(ip string, c interface{}) error {
	v := reflect.ValueOf(c)
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("Should pass in pointer")
	}
	uri := fmt.Sprintf("%s%s", uri.CfgUriRoot, composeCfgEntryPath(v))
	url := &util.HttpUrl{
		IP:   ip,
		Port: CfgServerPort,
		Uri:  uri,
	}
	s, err := util.HttpGet(url)
	if err != nil {
		log.Error("Fail to pullcfg from %s, %v", url.String(), err)
		return err
	}
	log.Info("PullCfg for %s, get:\n%s", v.Elem().Type().Name(), s)
	if err := json.Unmarshal([]byte(s), c); err != nil {
		return err
	}
	return nil
}
