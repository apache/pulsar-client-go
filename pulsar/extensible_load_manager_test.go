// //go:build extensible_load_manager

package pulsar

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/apache/pulsar-client-go/pulsaradmin"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"

	uAtomic "go.uber.org/atomic"
)

type ExtensibleLoadManagerTestSuite struct {
	suite.Suite
}

func TestExtensibleLoadManagerTestSuite(t *testing.T) {
	suite.Run(t, new(ExtensibleLoadManagerTestSuite))
}

const (
	tenant    = utils.PUBLICTENANT
	namespace = utils.DEFAULTNAMESPACE

	broker1URL = "pulsar://broker-1:6650"
	broker2URL = "pulsar://broker-2:6650"

	broker1LookupURL = "broker-1:8080"
	broker2LookupURL = "broker-2:8080"
)

type mockCounter struct {
	prometheus.Counter

	count uAtomic.Int32
}

func (m *mockCounter) Inc() {
	m.count.Inc()
}

func (suite *ExtensibleLoadManagerTestSuite) TestTopicUnloadWithAssignedUrl() {
	suite.T().Skipf("Skipping test until proxy issue is solved")
	assertions := suite.Assert()

	admin, err := pulsaradmin.NewClient(&pulsaradmin.Config{WebServiceURL: "http://broker-1:8080"})
	assertions.Nil(err)

	topicName, err := utils.GetTopicName(newTopicName())
	assertions.Nil(err)
	assertions.NotNil(topicName)

	err = admin.Topics().Create(*topicName, 0)
	assertions.Nil(err)

	lookupResult, err := admin.Topics().Lookup(*topicName)
	assertions.Nil(err)
	assertions.NotEmpty(lookupResult.BrokerURL)
	srcTopicBrokerURL := lookupResult.BrokerURL
	assertions.Contains([...]string{broker1URL, broker2URL}, srcTopicBrokerURL)

	var dstTopicBrokerURL string
	if srcTopicBrokerURL == broker1URL {
		dstTopicBrokerURL = broker2LookupURL
	} else {
		dstTopicBrokerURL = broker1LookupURL
	}

	bundleRange, err := admin.Topics().GetBundleRange(*topicName)
	assertions.Nil(err)
	assertions.NotEmpty(bundleRange)

	pulsarClient, err := NewClient(ClientOptions{URL: srcTopicBrokerURL})
	assertions.Nil(err)
	defer pulsarClient.Close()

	producer, err := pulsarClient.CreateProducer(ProducerOptions{
		Topic: topicName.String(),
	})
	assertions.Nil(err)
	defer producer.Close()

	consumer, err := pulsarClient.Subscribe(ConsumerOptions{
		Topic:                       topicName.String(),
		SubscriptionName:            fmt.Sprintf("my-sub-%v", time.Now().Nanosecond()),
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assertions.Nil(err)
	defer consumer.Close()

	pulsarClientImpl := pulsarClient.(*client)
	lookupRequestCounterMock := mockCounter{}
	pulsarClientImpl.metrics.LookupRequestsCount = &lookupRequestCounterMock

	messageCountBeforeUnload := 100
	messageCountDuringUnload := 100
	messageCountAfterUnload := 100
	messageCount := messageCountBeforeUnload + messageCountDuringUnload + messageCountAfterUnload

	// Signals all goroutines have completed
	wgRoutines := sync.WaitGroup{}
	wgRoutines.Add(2)

	// Signals unload has completed
	wgUnload := sync.WaitGroup{}
	wgUnload.Add(1)

	// Signals both producer and consumer have processed `messageCountBeforeUnload` messages
	wgSendAndReceiveMessages := sync.WaitGroup{}
	wgSendAndReceiveMessages.Add(2)

	// Producer
	go func() {
		defer wgRoutines.Done()

		for i := 0; i < messageCount; i++ {
			if i == messageCountBeforeUnload+messageCountDuringUnload {
				wgUnload.Wait()
			}

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			pm := ProducerMessage{Payload: []byte(fmt.Sprintf("hello-%d", i))}
			_, err := producer.Send(ctx, &pm)
			assertions.Nil(err)
			assertions.Nil(ctx.Err())

			if i == messageCountBeforeUnload {
				wgSendAndReceiveMessages.Done()
			}
		}
	}()

	// Consumer
	go func() {
		defer wgRoutines.Done()

		for i := 0; i < messageCount; i++ {
			if i == messageCountBeforeUnload+messageCountDuringUnload {
				wgUnload.Wait()
			}

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			_, err := consumer.Receive(ctx)
			assertions.Nil(err)
			assertions.Nil(ctx.Err())

			if i == messageCountBeforeUnload {
				wgSendAndReceiveMessages.Done()
			}
		}
	}()

	// Unload the bundle, triggering the producers and consumers to reconnect to the specified broker.
	wgSendAndReceiveMessages.Wait()
	unloadURL := fmt.Sprintf(
		"/admin/v2/namespaces/%s/%s/%s/unload?destinationBroker=%s", tenant, namespace, bundleRange, dstTopicBrokerURL)
	makeHTTPCall(suite.T(), http.MethodPut, lookupResult.HTTPURL+unloadURL, "")
	wgUnload.Done()

	wgRoutines.Wait()
	assertions.Equal(int32(0), lookupRequestCounterMock.count.Load())
}

func (suite *ExtensibleLoadManagerTestSuite) TestTopicUnloadWithAssignedUrlAndProxy() {
	assertions := suite.Assert()

	admin, err := pulsaradmin.NewClient(&pulsaradmin.Config{WebServiceURL: "http://proxy:8080"})
	assertions.Nil(err)

	topicName, err := utils.GetTopicName(newTopicName())
	assertions.Nil(err)
	assertions.NotNil(topicName)

	err = admin.Topics().Create(*topicName, 0)
	assertions.Nil(err)

	lookupResult, err := admin.Topics().Lookup(*topicName)
	assertions.Nil(err)
	assertions.NotEmpty(lookupResult.BrokerURL)
	srcTopicBrokerURL := lookupResult.BrokerURL
	assertions.Contains([...]string{broker1URL, broker2URL}, srcTopicBrokerURL)

	var dstTopicBrokerURL string
	if srcTopicBrokerURL == broker1URL {
		dstTopicBrokerURL = broker2LookupURL
	} else {
		dstTopicBrokerURL = broker1LookupURL
	}

	bundleRange, err := admin.Topics().GetBundleRange(*topicName)
	assertions.Nil(err)
	assertions.NotEmpty(bundleRange)

	logrusLogger := logrus.New()
	logrusLogger.SetLevel(logrus.DebugLevel)

	pulsarClient, err := NewClient(ClientOptions{
		URL:    "pulsar://proxy:6650",
		Logger: log.NewLoggerWithLogrus(logrusLogger),
	})
	assertions.Nil(err)
	defer pulsarClient.Close()

	producer, err := pulsarClient.CreateProducer(ProducerOptions{
		Topic: topicName.String(),
	})
	assertions.Nil(err)
	defer producer.Close()

	consumer, err := pulsarClient.Subscribe(ConsumerOptions{
		Topic:                       topicName.String(),
		SubscriptionName:            fmt.Sprintf("my-sub-%v", time.Now().Nanosecond()),
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assertions.Nil(err)
	defer consumer.Close()

	pulsarClientImpl := pulsarClient.(*client)
	lookupRequestCounterMock := mockCounter{}
	pulsarClientImpl.metrics.LookupRequestsCount = &lookupRequestCounterMock

	connectionPool := pulsarClientImpl.cnxPool
	assertions.Equal(1, internal.GetConnectionsCount(&connectionPool))

	messageCountBeforeUnload := 1
	messageCountDuringUnload := 1
	messageCountAfterUnload := 1
	messageCount := messageCountBeforeUnload + messageCountDuringUnload + messageCountAfterUnload

	// Signals all goroutines have completed
	wgRoutines := sync.WaitGroup{}
	wgRoutines.Add(2)

	// Signals unload has completed
	wgUnload := sync.WaitGroup{}
	wgUnload.Add(1)

	// Signals both producer and consumer have processed `messageCountBeforeUnload` messages
	wgSendAndReceiveMessages := sync.WaitGroup{}
	wgSendAndReceiveMessages.Add(2)

	// Producer
	go func() {
		defer wgRoutines.Done()

		for i := 0; i < messageCount; i++ {
			if i == messageCountBeforeUnload+messageCountDuringUnload {
				wgUnload.Wait()
			}

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			pm := ProducerMessage{Payload: []byte(fmt.Sprintf("hello-%d", i))}
			_, err := producer.Send(ctx, &pm)
			assertions.Nil(err)
			assertions.Nil(ctx.Err())

			if i == messageCountBeforeUnload {
				wgSendAndReceiveMessages.Done()
			}
		}
	}()

	// Consumer
	go func() {
		defer wgRoutines.Done()

		for i := 0; i < messageCount; i++ {
			if i == messageCountBeforeUnload+messageCountDuringUnload {
				wgUnload.Wait()
			}

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			_, err := consumer.Receive(ctx)
			assertions.Nil(err)
			assertions.Nil(ctx.Err())

			if i == messageCountBeforeUnload {
				wgSendAndReceiveMessages.Done()
			}
		}
	}()

	// Unload the bundle, triggering the producers and consumers to reconnect to the specified broker.
	wgSendAndReceiveMessages.Wait()
	unloadURL := fmt.Sprintf(
		"/admin/v2/namespaces/%s/%s/%s/unload?destinationBroker=%s", tenant, namespace, bundleRange, dstTopicBrokerURL)
	makeHTTPCall(suite.T(), http.MethodPut, lookupResult.HTTPURL+unloadURL, "")
	wgUnload.Done()

	wgRoutines.Wait()
	assertions.Equal(int32(0), lookupRequestCounterMock.count.Load())

	// We are connecting via a proxy, but have direct connectivity to the brokers.
	// Validate the client stayed connected through the proxy only.
	assertions.Equal(1, internal.GetConnectionsCount(&connectionPool))
}
