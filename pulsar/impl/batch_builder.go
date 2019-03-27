package impl

type BatchBuilder struct {
	buffer Buffer
}

func NewBatchBuilder() *BatchBuilder {
	return &BatchBuilder{
		buffer: NewBuffer(4096),
	}
}

func (bb *BatchBuilder) isFull() bool {
	return false
}

func (bb *BatchBuilder) hasSpace(size int) bool {
	return false
}

func (bb *BatchBuilder) Flush() []byte {
	return nil
}
