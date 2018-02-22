package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func Max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func Min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func SplitAddr(addr string) (ip string, port int, err error) {
	toks := strings.Split(addr, ":")
	if len(toks) != 2 {
		err = fmt.Errorf("Fail to split %q, get %v", addr, toks)
		return
	}
	ip = toks[0]
	port64, err := strconv.ParseInt(toks[1], 10, 32)
	if err != nil {
		err = fmt.Errorf("Fail to parse port from %q, %v", addr, err)
		return
	}
	port = int(port64)
	return
}

func PeriodicalRoutine(skipFirst bool,
	interval time.Duration, routine func(interface{}), args interface{}) {
	if skipFirst {
		time.Sleep(interval)
	}
	for {
		t1 := time.Now().Add(interval)
		routine(args)
		t2 := time.Now()
		if t2.Before(t1) {
			time.Sleep(t1.Sub(t2))
		}
	}
}

type PrettyTable struct {
	header      []string
	lines       [][]string
	colSep      string
	colMaxWidth []int
}

func (t *PrettyTable) Init(header []string) {
	t.header = header
	t.lines = make([][]string, 0)
	t.colSep = "  "
	t.colMaxWidth = make([]int, len(header))
	for i, s := range header {
		t.colMaxWidth[i] = len(s)
	}
}

func (t *PrettyTable) AppendLine(line []string) {
	t.lines = append(t.lines, line)
	for i, s := range line {
		t.colMaxWidth[i] = Max(t.colMaxWidth[i], len(s))
	}
}

func (t *PrettyTable) Format() string {
	buf := bytes.NewBuffer(nil)
	t.formatHeader(buf)
	t.formatSep(buf)
	for _, line := range t.lines {
		t.formatLine(buf, line)
	}
	return buf.String()
}

func (t *PrettyTable) formatHeader(buf *bytes.Buffer) {
	t.formatLine(buf, t.header)
}

func (t *PrettyTable) formatSep(buf *bytes.Buffer) {
	cnt := 0
	for i, w := range t.colMaxWidth {
		cnt += w
		if i > 0 {
			cnt += len(t.colSep)
		}
	}
	for i := 0; i < cnt; i++ {
		buf.WriteByte('-')
	}
	buf.WriteByte('\n')
}

func (t *PrettyTable) formatLine(buf *bytes.Buffer, line []string) {
	for i, col := range line {
		if i > 0 {
			buf.WriteString(t.colSep)
		}
		buf.WriteString(fmt.Sprintf("%*s", -t.colMaxWidth[i], col))
	}
	buf.WriteByte('\n')
}

func FitDataInto(data interface{}, v interface{}) error {
	buf, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(buf, v); err != nil {
		return err
	}
	return nil
}
