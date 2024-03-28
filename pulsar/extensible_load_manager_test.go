//go:build extensible_load_manager

package pulsar

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsaradmin"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
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

func (suite *ExtensibleLoadManagerTestSuite) TestTopicUnload() {
	type topicUnloadTestCase struct {
		testCaseName       string
		adminURL           string
		clientEndpointFunc func(utils.LookupData) string
		unloadEndpointFunc func(utils.LookupData) string
	}
	for _, scenario := range []topicUnloadTestCase{
		{
			testCaseName: "directConnection",
			adminURL:     "http://broker-1:8080",
			clientEndpointFunc: func(lookupResult utils.LookupData) string {
				return lookupResult.BrokerURL
			},
			unloadEndpointFunc: func(lookupResult utils.LookupData) string {
				return lookupResult.HTTPURL
			},
		},
		{
			testCaseName: "proxyConnection",
			adminURL:     "http://proxy:8080",
			clientEndpointFunc: func(utils.LookupData) string {
				return "pulsar://proxy:6650"
			},
			unloadEndpointFunc: func(utils.LookupData) string {
				return "http://proxy:8080"
			},
		},
	} {
		suite.T().Run(scenario.testCaseName, func(t *testing.T) {
			testTopicUnload(t, scenario.adminURL, scenario.clientEndpointFunc, scenario.unloadEndpointFunc)
		})
	}
}

func testTopicUnload(t *testing.T, adminURL string,
	clientEndpointFunc func(utils.LookupData) string,
	unloadEndpointFunc func(utils.LookupData) string) {
	req := assert.New(t)

	admin, err := pulsaradmin.NewClient(&pulsaradmin.Config{WebServiceURL: adminURL})
	req.NoError(err)

	topicName, err := utils.GetTopicName(newTopicName())
	req.NoError(err)
	req.NotNil(topicName)

	err = admin.Topics().Create(*topicName, 0)
	req.NoError(err)

	lookupResult, err := admin.Topics().Lookup(*topicName)
	req.NoError(err)
	req.NotEmpty(lookupResult.BrokerURL)
	srcTopicBrokerURL := lookupResult.BrokerURL
	req.Contains([...]string{broker1URL, broker2URL}, srcTopicBrokerURL)

	var dstTopicBrokerURL string
	if srcTopicBrokerURL == broker1URL {
		dstTopicBrokerURL = broker2LookupURL
	} else {
		dstTopicBrokerURL = broker1LookupURL
	}

	bundleRange, err := admin.Topics().GetBundleRange(*topicName)
	req.NoError(err)
	req.NotEmpty(bundleRange)

	clientURL := clientEndpointFunc(lookupResult)
	pulsarClient, err := NewClient(ClientOptions{URL: clientURL})
	req.NoError(err)
	defer pulsarClient.Close()

	producer, err := pulsarClient.CreateProducer(ProducerOptions{
		Topic: topicName.String(),
	})
	req.NoError(err)
	defer producer.Close()

	consumer, err := pulsarClient.Subscribe(ConsumerOptions{
		Topic:                       topicName.String(),
		SubscriptionName:            fmt.Sprintf("my-sub-%v", time.Now().Nanosecond()),
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	req.NoError(err)
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
			req.NoError(err)
			req.NoError(ctx.Err())

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
			req.NoError(err)
			req.NoError(ctx.Err())

			if i == messageCountBeforeUnload {
				wgSendAndReceiveMessages.Done()
			}
		}
	}()

	// Unload the bundle, triggering the producers and consumers to reconnect to the specified broker.
	wgSendAndReceiveMessages.Wait()
	unloadEndpoint := unloadEndpointFunc(lookupResult)
	unloadURL := fmt.Sprintf(
		"/admin/v2/namespaces/%s/%s/%s/unload?destinationBroker=%s", tenant, namespace, bundleRange, dstTopicBrokerURL)
	makeHTTPCall(t, http.MethodPut, unloadEndpoint+unloadURL, "")
	wgUnload.Done()

	wgRoutines.Wait()
	req.Equal(int32(0), lookupRequestCounterMock.count.Load())
}
