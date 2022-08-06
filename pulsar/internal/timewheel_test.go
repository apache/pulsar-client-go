package internal

import (
	"fmt"
	"testing"
	"time"
)

func checkTimeCost(t *testing.T, start, end time.Time, before int, after int) bool {
	due := end.Sub(start)
	if due > time.Duration(after)*time.Millisecond {
		t.Error("delay run")
		return false
	}

	if due < time.Duration(before)*time.Millisecond {
		t.Error("run ahead")
		return false
	}

	return true
}

func TestCalcPos(t *testing.T) {
	tw := NewTimeWheel(100*time.Millisecond, 5)
	idx, round := tw.getPositionAndCircle(1 * time.Second)
	if round != 2 {
		t.Error("round err", round)
	}
	if idx != 0 {
		t.Error("idx err", idx)
	}
}

func TestAddFunc(t *testing.T) {
	tw := NewTimeWheel(100*time.Millisecond, 5)
	tw.Start()
	defer tw.Stop()

	for index := 1; index < 6; index++ {
		queue := make(chan bool)
		start := time.Now()
		tw.AddJob(fmt.Sprintf("key_%d", index), time.Duration(index)*time.Second, func() {
			queue <- true
		})

		<-queue

		before := index*1000 - 200
		after := index*1000 + 200
		checkTimeCost(t, start, time.Now(), before, after)
		t.Log("time since: ", time.Since(start).String())
	}
}

func TestRemove(t *testing.T) {
	tw := NewTimeWheel(100*time.Millisecond, 5)
	tw.Start()
	defer tw.Stop()

	queue := make(chan bool)
	tw.AddJob("key", time.Millisecond*500, func() {
		queue <- true
	})

	// remove action after add action
	time.AfterFunc(time.Millisecond*10, func() {
		tw.RemoveJob("key")
	})

	exitTimer := time.NewTimer(1 * time.Second)
	select {
	case <-exitTimer.C:
	case <-queue:
		t.Error("must not run")
	}
}
