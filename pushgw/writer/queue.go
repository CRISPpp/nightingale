package writer

import (
	"container/list"
	"sync"
	"time"

	"github.com/prometheus/prometheus/prompb"
	"github.com/toolkits/pkg/logger"
)

type SafeList struct {
	sync.RWMutex
	L *list.List
}

type MetricCount struct {
	MaxCountPerMinute int
	Count int
	LastTimeStamp int64
	Mutex sync.Mutex
}

var MC *MetricCount = &MetricCount{
	MaxCountPerMinute: 10000,
	Count: 0,
	LastTimeStamp: time.Now().Unix(),
}

func NewSafeList() *SafeList {
	return &SafeList{L: list.New()}
}

func (sl *SafeList) PushFront(v interface{}) *list.Element {

	sl.Lock()
	e := sl.L.PushFront(v)
	sl.Unlock()
	return e
}

func (sl *SafeList) PushFrontBatch(vs []interface{}) {
	sl.Lock()
	for _, item := range vs {
		sl.L.PushFront(item)
	}
	sl.Unlock()
}

func (sl *SafeList) PopBack(max int) []prompb.TimeSeries {
	sl.Lock()

	count := sl.L.Len()
	if count == 0 {
		sl.Unlock()
		return []prompb.TimeSeries{}
	}

	if count > max {
		count = max
	}

	items := make([]prompb.TimeSeries, 0, count)
	for i := 0; i < count; i++ {
		item := sl.L.Remove(sl.L.Back())
		sample, ok := item.(prompb.TimeSeries)
		if ok {
			items = append(items, sample)
		}
	}

	sl.Unlock()
	return items
}

func (sl *SafeList) RemoveAll() {
	sl.Lock()
	sl.L.Init()
	sl.Unlock()
}

func (sl *SafeList) Len() int {
	sl.RLock()
	size := sl.L.Len()
	sl.RUnlock()
	return size
}

// SafeList with Limited Size
type SafeListLimited struct {
	maxSize int
	SL      *SafeList
}

func NewSafeListLimited(maxSize int) *SafeListLimited {
	return &SafeListLimited{SL: NewSafeList(), maxSize: maxSize}
}

func (sll *SafeListLimited) PopBack(max int) []prompb.TimeSeries {
	return sll.SL.PopBack(max)
}

func (sll *SafeListLimited) PushFront(v interface{}) bool {
	MC.Mutex.Lock()
	defer MC.Mutex.Unlock()

	if MC.Count > MC.MaxCountPerMinute {
		logger.Warningf("metric count exceed %d, cur count: %d metrics", MC.MaxCountPerMinute, MC.Count)
		return false
	}

	if sll.SL.Len() >= sll.maxSize {
		return false
	}

	sll.SL.PushFront(v)
	MC.Count += 1
	
	return true
}

func (sll *SafeListLimited) PushFrontBatch(vs []interface{}) bool {
	MC.Mutex.Lock()
	defer MC.Mutex.Unlock()

	if MC.Count > MC.MaxCountPerMinute {
		logger.Warningf("metric count exceed %d, cur count: %d metrics", MC.MaxCountPerMinute, MC.Count)
		return false
	}

	if sll.SL.Len() >= sll.maxSize {
		return false
	}

	if MC.Count + len(vs) > MC.MaxCountPerMinute {
		overCnt := MC.Count + len(vs) - MC.MaxCountPerMinute
		logger.Warningf("metric count exceed %d, over count: %d metrics", MC.MaxCountPerMinute, overCnt)
		sll.SL.PushFrontBatch(vs[:len(vs)-overCnt])
		MC.Count += len(vs) - overCnt
		return true
	}

	sll.SL.PushFrontBatch(vs)
	MC.Count += len(vs)
	return true
}

func (sll *SafeListLimited) RemoveAll() {
	sll.SL.RemoveAll()
}

func (sll *SafeListLimited) Len() int {
	return sll.SL.Len()
}
