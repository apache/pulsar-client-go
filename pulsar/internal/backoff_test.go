package internal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	ms  = time.Millisecond
	sec = time.Second
)

// assert actualDur within [0.9*expectedDur, 1.1*expectedDur]
func assertInDelta(t *testing.T, expectedDur, actualDur time.Duration) {
	assert.InDelta(t, expectedDur, actualDur, float64(expectedDur/10))
}

// nowMocker make sure now time for backoff is deterministic without waiting
type mockedNow struct {
	t time.Time
}

func newMockedNow() *mockedNow {
	mock := new(mockedNow)
	systemNow = mock.now // overwrite backoff system now time provider
	return mock
}

func (m *mockedNow) now() time.Time { return m.t }

func (m *mockedNow) set(d time.Duration) {
	m.t = time.Time{}.Add(d) // relative empty start time is unrelated with the backoff
}

func TestMandatoryStop(t *testing.T) {
	b := NewBackoff(100*ms, 60*sec, 1900*ms)
	mock := newMockedNow()

	mock.set(0)
	assertInDelta(t, 100*ms, b.Next())
	mock.set(100 * ms)
	assertInDelta(t, 200*ms, b.Next())
	mock.set(300 * ms)
	assertInDelta(t, 400*ms, b.Next())
	mock.set(700 * ms)
	assertInDelta(t, 800*ms, b.Next())
	mock.set(1500 * ms)

	// current elapse is 1.5s, next retry gap is 1.6s
	// so next elapse is 1.5s+1.6s=3.1s since first retry
	// 3.1s greater than mandatory stop (1.9s), reset backoff to max(0.1s, 1.9s-1.5s) aka 0.4s
	assertInDelta(t, 400*ms, b.Next())
	mock.set(1900 * ms)
	// from now on, mandatory stop policy is invalid

	assertInDelta(t, 3200*ms, b.Next())
	mock.set(1901 * ms)
	assertInDelta(t, 6400*ms, b.Next())
	assertInDelta(t, 12800*ms, b.Next())
	assertInDelta(t, 25600*ms, b.Next())
	assertInDelta(t, 51200*ms, b.Next())

	// reached max 60s backoff
	assertInDelta(t, 60000*ms, b.Next())
	assertInDelta(t, 60000*ms, b.Next())
}

func TestMandatoryStopUnreached(t *testing.T) {
	b := NewBackoff(100*ms, 60*sec, 1900*ms)
	assertInDelta(t, 100*ms, b.Next())
	assertInDelta(t, 200*ms, b.Next())
	assertInDelta(t, 400*ms, b.Next())
	assertInDelta(t, 800*ms, b.Next())

	// not reach mandatory stop yet
	assertInDelta(t, 1600*ms, b.Next())
}

func TestBackOffMax(t *testing.T) {
	b := NewBackoff(5*ms, 20*ms, 39*ms)
	mock := newMockedNow()

	mock.set(0)
	assert.Equal(t, 5*ms, b.Next())

	mock.set(10 * ms)
	assertInDelta(t, 10*ms, b.Next())
	mock.set(20 * ms)
	assertInDelta(t, 19*ms, b.Next())
	mock.set(40 * ms)
	assertInDelta(t, 20*ms, b.Next())

	assertInDelta(t, 20*ms, b.Next())
}
