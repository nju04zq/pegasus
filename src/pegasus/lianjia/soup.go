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

func formatTag(args ...string) string {
	if len(args) == 0 || len(args)%2 == 0 {
		return ""
	}
	buf := bytes.NewBuffer(nil)
	buf.WriteString(fmt.Sprintf("<%s", args[0]))
	for i := 1; i < len(args); i += 2 {
		buf.WriteString(fmt.Sprintf(` "%s"="%s"`, args[i], args[i+1]))
	}
	buf.WriteRune('>')
	return buf.String()
}

func findAll(tag *soup.Root, minCnt, maxCnt int, args ...string) ([]soup.Root, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("No args provided")
	}
	name := args[0]
	tags := tag.FindAll(args...)
	if (minCnt > 0 && len(tags) < minCnt) || (maxCnt > 0 && len(tags) > maxCnt) {
		log.Error("Tag %s mismatch\n%s", name, render(tag))
		return nil, fmt.Errorf("Tag %s mismatch [%d, %d]",
			formatTag(args...), minCnt, maxCnt)
	}
	return tags, nil
}

func tagAttr(tag *soup.Root, key string) (string, error) {
	attrs := tag.Attrs()
	if val, ok := attrs[key]; ok {
		return val, nil
	} else {
		return "", fmt.Errorf("Attr %q not found for tag %s", key, render(tag))
	}
}
