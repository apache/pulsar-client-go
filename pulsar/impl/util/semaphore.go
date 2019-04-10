package util

type Semaphore chan bool

func (s Semaphore) Acquire() {
	s <- true
}

func (s Semaphore) Release() {
	<-s
}
