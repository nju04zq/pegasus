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

func regionLink(r *Region) string {
	return fmt.Sprintf("%s%s/", ERSHOUFANG_LINK, r.Abbr)
}

func regionPgLink(r *Region, page int) string {
	link := regionLink(r)
	// pg2co32, 最新排序
	return fmt.Sprintf("%spg%dco32/", link, page)
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

func getAidFromHref(href string) (string, error) {
	if !strings.HasPrefix(href, ERSHOUFANG_LINK) {
		return "", fmt.Errorf("Apartment href %q not starts with %q", href, ERSHOUFANG_LINK)
	}
	html := ".html"
	if !strings.HasSuffix(href, html) {
		return "", fmt.Errorf("Apartment href %q not ends with %q", href, html)
	}
	href = strings.TrimPrefix(href, ERSHOUFANG_LINK)
	href = strings.TrimSuffix(href, html)
	return href, nil
}
