package lianjia

import (
	"fmt"
	"strings"
)

const (
	LIANJIA_LINK    = "https://sh.lianjia.com"
	ERSHOUFANG_LINK = LIANJIA_LINK + "/ershoufang/"
)

func distLink(d *District) string {
	return fmt.Sprintf("%s%s/", ERSHOUFANG_LINK, d.Abbr)
}

func regionLink(regionUri string) string {
	return fmt.Sprintf("%s%s", LIANJIA_LINK, regionUri)
}

func parseAbbr(uri string) (string, error) {
	if !strings.HasPrefix(uri, "/") || !strings.HasPrefix(uri, "/") {
		return "", fmt.Errorf("Region URI %q not start/end with /", uri)
	}
	uri = strings.TrimPrefix(uri, "/")
	uri = strings.TrimSuffix(uri, "/")
	toks := strings.Split(uri, "/")
	if len(toks) != 2 {
		return "", fmt.Errorf("Region URI %q not has 2 toks", uri)
	}
	return toks[1], nil
}
