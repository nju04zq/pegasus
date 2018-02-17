package lianjia

import "fmt"

const (
	LIANJIA_LINK    = "https://sh.lianjia.com"
	ERSHOUFANG_LINK = LIANJIA_LINK + "/ershoufang/"
)

func distLink(distUri string) string {
	return fmt.Sprintf("%s%s", LIANJIA_LINK, distUri)
}
