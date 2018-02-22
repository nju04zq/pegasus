package lianjia

import (
	"bytes"
	"fmt"
	"pegasus/log"

	"github.com/anaskhan96/soup"
	"golang.org/x/net/html"
)

func render(tag *soup.Root) string {
	buf := bytes.NewBuffer(nil)
	html.Render(buf, tag.Pointer)
	return buf.String()
}

func findAll(tag *soup.Root, minCnt, maxCnt int, args ...string) ([]soup.Root, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("No args provided")
	}
	name := args[0]
	tags := tag.FindAll(args...)
	if (minCnt > 0 && len(tags) < minCnt) || (maxCnt > 0 && len(tags) > maxCnt) {
		log.Debug("Tag %q mismatch\n%s", name, render(tag))
		return nil, fmt.Errorf("Tag %q mismatch [%d, %d]", name, minCnt, maxCnt)
	}
	return tags, nil
}
