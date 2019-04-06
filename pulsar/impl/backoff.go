package impl

import (
	"time"
)

type Backoff struct {
	backoff time.Duration
}

const minBackoff = 100 * time.Millisecond
const maxBackoff = 60 * time.Second

func (b *Backoff) Next() time.Duration {
	// Double the delay each time
	b.backoff += b.backoff
	if b.backoff.Nanoseconds() < minBackoff.Nanoseconds() {
		b.backoff = minBackoff
	} else if b.backoff.Nanoseconds() > maxBackoff.Nanoseconds() {
		b.backoff = maxBackoff
	}

	return b.backoff
}
