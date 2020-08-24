package pulsar

import (
	"context"
	"errors"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	log "github.com/sirupsen/logrus"
)

const (
	DlqTopicSuffix    = "-DLQ"
	RetryTopicSuffix  = "-RETRY"
	MaxReconsumeTimes = 16

	SysPropertyDelayTime       = "DELAY_TIME"
	SysPropertyRealTopic       = "REAL_TOPIC"
	SysPropertyRetryTopic      = "RETRY_TOPIC"
	SysPropertyReconsumeTimes  = "RECONSUMETIMES"
	SysPropertyOriginMessageID = "ORIGIN_MESSAGE_IDY_TIME"
)

type RetryMessage struct {
	producerMsg ProducerMessage
	consumerMsg ConsumerMessage
}

type retryRouter struct {
	client    Client
	producer  Producer
	policy    *DLQPolicy
	messageCh chan RetryMessage
	closeCh   chan interface{}
	log       *log.Entry
}

func newRetryRouter(client Client, policy *DLQPolicy) (*retryRouter, error) {
	r := &retryRouter{
		client: client,
		policy: policy,
	}

	if policy != nil {
		if policy.MaxDeliveries <= 0 {
			return nil, errors.New("DLQPolicy.MaxDeliveries needs to be > 0")
		}

		if policy.RetryLetterTopic == "" {
			return nil, errors.New("DLQPolicy.RetryLetterTopic needs to be set to a valid topic name")
		}

		r.messageCh = make(chan RetryMessage)
		r.closeCh = make(chan interface{}, 1)
		r.log = log.WithField("rlq-topic", policy.RetryLetterTopic)
		go r.run()
	}
	return r, nil
}

func (r *retryRouter) Chan() chan RetryMessage {
	return r.messageCh
}

func (r *retryRouter) run() {
	for {
		select {
		case rm := <-r.messageCh:
			r.log.WithField("msgID", rm.consumerMsg.ID()).Debug("Got message for RLQ")
			producer := r.getProducer()

			msgID := rm.consumerMsg.ID()
			producer.SendAsync(context.Background(), &rm.producerMsg, func(MessageID, *ProducerMessage, error) {
				// TODO: if router produce failed, should Nack this message
				r.log.WithField("msgID", msgID).Debug("Sent message to RLQ")
				rm.consumerMsg.Consumer.AckID(msgID)
			})

		case <-r.closeCh:
			if r.producer != nil {
				r.producer.Close()
			}
			r.log.Debug("Closed RLQ router")
			return
		}
	}
}

func (r *retryRouter) close() {
	// Attempt to write on the close channel, without blocking
	select {
	case r.closeCh <- nil:
	default:
	}
}

func (r *retryRouter) getProducer() Producer {
	if r.producer != nil {
		// Producer was already initialized
		return r.producer
	}

	// Retry to create producer indefinitely
	backoff := &internal.Backoff{}
	for {
		producer, err := r.client.CreateProducer(ProducerOptions{
			Topic:                   r.policy.RetryLetterTopic,
			CompressionType:         LZ4,
			BatchingMaxPublishDelay: 100 * time.Millisecond,
		})

		if err != nil {
			r.log.WithError(err).Error("Failed to create RLQ producer")
			time.Sleep(backoff.Next())
			continue
		} else {
			r.producer = producer
			return producer
		}
	}
}
