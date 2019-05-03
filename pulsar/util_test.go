package pulsar

import (
	"fmt"
	"time"
)

const serviceUrl = "pulsar://localhost:6650"

func newTopicName() string {
	return fmt.Sprintf("my-topic-%v", time.Now().Nanosecond())
}
