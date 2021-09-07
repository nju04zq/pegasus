package lianjia

import (
	"testing"

	"pegasus/log"
	"pegasus/rate"

	"github.com/anaskhan96/soup"
)

type dummyTaskletCtx struct {
}

func (t *dummyTaskletCtx) Close() {
}

func TestGetApartments(t *testing.T) {
	tasklet := &taskletGetApartments{
		region: &Region{
			Abbr: "gumei",
		},
		page: 2,
	}
	ctx := new(dummyTaskletCtx)
	if err := tasklet.Execute(ctx); err != nil {
		t.Fatalf("tasklet execute fail, %v", err)
	}
	t.Logf("get apartments %d", len(tasklet.apartments))
	for _, apartment := range tasklet.apartments {
		t.Logf("apartment %+v", apartment)
	}
}

func TestGetOneApartment(t *testing.T) {
	tasklet := &taskletGetApartments{
		region: &Region{
			Abbr: "gumei",
		},
		page: 2,
	}
	link := regionPgLink(tasklet.region, tasklet.page)
	resp, err := rate.GetHtml(link)
	if err != nil {
		t.Fatalf("Fail to get apartments from %q, %v", link, err)
	}
	doc := soup.HTMLParse(resp)
	tags, err := findAll(&doc, 0, 0, "div", "class", "info clear")
	if err != nil {
		t.Fatalf("Fail to get apartment list")
	}
	tag := tags[1]
	apartment, err := tasklet.parseApartment(&tag)
	if err != nil {
		t.Fatalf("Fail to parse apartment in %s, %s, %v", link, render(&tag), err)
	}
	t.Logf("Get Apartment as %+v", apartment)
}

func init() {
	log.RegisterLogger(&log.ConsoleLogger{
		Level: log.LevelDebug,
	})
}

/*

// 2021/09/07

<div class="info clear"><div class="title"><a class="" href="https://sh.lianjia.com/ershoufang/107104326866.html" target="_blank" data-log_index="2" data-el="ershoufang" data-housecode="107104326866" data-is_focus="" data-sl="">南方新村 2室1厅 西南</a><!-- 拆分标签 只留一个优先级最高的标签--></div><div class="flood"><div class="positionInfo"><span class="positionIcon"></span><a href="https://sh.lianjia.com/xiaoqu/5011000014585/" target="_blank" data-log_index="2" data-el="region">南方新村 </a>   -  <a href="https://sh.lianjia.com/ershoufang/gumei/" target="_blank">古美</a> </div></div><div class="address"><div class="houseInfo"><span class="houseIcon"></span>2室1厅 | 55.79平米 | 西南 | 简装 | 高楼层(共6层) | 1994年建 | 板楼</div></div><div class="followInfo"><span class="starIcon"></span>13人关注 / 23天以前发布</div><div class="tag"><span class="subway">近地铁</span><span class="vr">VR房源</span><span class="taxfree">房本满五年</span><span class="haskey">随时看房</span></div><div class="priceInfo"><div class="totalPrice totalPrice2"><i> </i><span>404.1</span><i>万</i></div><div class="unitPrice" data-hid="107104326866" data-rid="5011000014585" data-price="72433"><span>72,433元/平</span></div></div></div>



<div class="info clear"><div class="title"><a class="" href="https://sh.lianjia.com/ershoufang/107104331661.html" target="_blank" data-log_index="1" data-el="ershoufang" data-housecode="107104331661" data-is_focus="" data-sl="">新上中间位置大2房，户型方正精装修，有车位单独出售</a><!-- 拆分标签 只留一个优先级最高的标签--><span class="yezhushuo tagBlock">房主自荐</span></div><div class="flood"><div class="positionInfo"><span class="positionIcon"></span><a href="https://sh.lianjia.com/xiaoqu/5011000016293/" target="_blank" data-log_index="1" data-el="region">蔚蓝城市花园 </a>   -  <a href="https://sh.lianjia.com/ershoufang/gumei/" target="_blank">古美</a> </div></div><div class="address"><div class="houseInfo"><span class="houseIcon"></span>2室2厅 | 96.34平米 | 南 北 | 精装 | 低楼层(共14层) | 2004年建 | 板楼</div></div><div class="followInfo"><span class="starIcon"></span>27人关注 / 22天以前发布</div><div class="tag"><span class="vr">VR房源</span><span class="taxfree">房本满五年</span></div><div class="priceInfo"><div class="totalPrice totalPrice2"><i> </i><span>890</span><i>万</i></div><div class="unitPrice" data-hid="107104331661" data-rid="5011000016293" data-price="92382"><span>92,382元/平</span></div></div></div>


*/
