//go:build extensible_load_manager

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package pulsar

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsaradmin"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type BlueGreenMigrationTestSuite struct {
	suite.Suite
}

func TestBlueGreenMigrationTestSuite(t *testing.T) {
	suite.Run(t, new(BlueGreenMigrationTestSuite))
}

func (suite *BlueGreenMigrationTestSuite) TestTopicMigration() {
	type topicUnloadTestCase struct {
		testCaseName  string
		blueAdminURL  string
		blueClientUrl string
		greenAdminURL string
		migrationBody string
	}
	for _, scenario := range []topicUnloadTestCase{

		{
			testCaseName: "proxyConnection",
			blueAdminURL: "http://localhost:8080",
			blueClientUrl: "pulsar://localhost:6650",
			greenAdminURL: "http://localhost:8081",
			migrationBody: `
						{
							"serviceUrl": "http://localhost:8081",
							"serviceUrlTls":"https://localhost:8085",
							"brokerServiceUrl": "pulsar://localhost:6651",
							"brokerServiceUrlTls": "pulsar+ssl://localhost:6655"
						}
					`,
		},
	} {
		suite.T().Run(scenario.testCaseName, func(t *testing.T) {
			testTopicMigrate(t,
				scenario.blueAdminURL, scenario.blueClientUrl, scenario.greenAdminURL, scenario.migrationBody)
		})
	}
}

func testTopicMigrate(
	t *testing.T,
	blueAdminURL string,
	blueClientUrl string,
	greenAdminURL string,
	migrationBody string) {
	runtime.GOMAXPROCS(1)
	const (
		cluster = "cluster-a"
		tenant    = utils.PUBLICTENANT
		namespace = utils.DEFAULTNAMESPACE

		blueBroker1URL = "pulsar://broker-1:6650"
		blueBroker2URL = "pulsar://broker-2:6650"
		greenBroker1URL = "pulsar://green-broker-1:6650"
		greenBroker2URL = "pulsar://green-broker-2:6650"

		blueBroker1LookupURL = "broker-1:8080"
		blueBroker2LookupURL = "broker-2:8080"
		greenBroker1LookupURL = "green-broker-1:8080"
		greenBroker2LookupURL = "green-broker-2:8080"
	)

	req := assert.New(t)

	topicMigrationURL := fmt.Sprintf(
		"/admin/v2/clusters/%s/migrate?migrated=false", cluster)
	makeHTTPCall(t, http.MethodPost, blueAdminURL+topicMigrationURL, migrationBody)

	admin, err := pulsaradmin.NewClient(&pulsaradmin.Config{WebServiceURL: blueAdminURL})
	req.NoError(err)

	topicName, err := utils.GetTopicName(newTopicName())
	req.NoError(err)
	req.NotNil(topicName)

	err = admin.Topics().Create(*topicName, 0)
	req.NoError(err)

	pulsarClient, err := NewClient(ClientOptions{URL: blueClientUrl})
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

			pm := ProducerMessage{Payload: []byte(fmt.Sprintf("hello-%d", i))}

			for true {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()
				_, err := producer.Send(ctx, &pm)
				if err == nil {
					break
				}
				time.Sleep(1000 * time.Millisecond)
			}

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

			for true {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()
				m, err := consumer.Receive(ctx)
				if err == nil {
					err = consumer.Ack(m)
					if err == nil {
						break
					}
				}
				time.Sleep(100 * time.Millisecond)
			}

			if i == messageCountBeforeUnload {
				wgSendAndReceiveMessages.Done()
			}
		}
	}()

	// Unload the bundle, triggering the producers and consumers to reconnect to the specified broker.
	wgSendAndReceiveMessages.Wait()

	topicMigrationURL = fmt.Sprintf(
		"/admin/v2/clusters/%s/migrate?migrated=true", cluster)
	makeHTTPCall(t, http.MethodPost, blueAdminURL+topicMigrationURL, migrationBody)

	greenAdmin, err := pulsaradmin.NewClient(&pulsaradmin.Config{WebServiceURL: greenAdminURL})
	srcTopicBrokerURL := ""
	retryAssert(t, 30, 2000, func() {
		lookupResult, _ := greenAdmin.Topics().Lookup(*topicName)
		srcTopicBrokerURL = lookupResult.BrokerURL
	}, func(x assert.TestingT) bool {
		return req.Contains([...]string{greenBroker1URL, greenBroker2URL}, srcTopicBrokerURL)
	})

	var dstTopicBrokerURL string
	var dstTopicBrokerLookupURL string
	if srcTopicBrokerURL == greenBroker1URL {
		dstTopicBrokerURL = greenBroker2URL
		dstTopicBrokerLookupURL = greenBroker2LookupURL
	} else {
		dstTopicBrokerURL = greenBroker1URL
		dstTopicBrokerLookupURL = greenBroker1LookupURL
	}

	bundleRange, err := greenAdmin.Topics().GetBundleRange(*topicName)
	req.NoError(err)
	req.NotEmpty(bundleRange)


	unloadURL := fmt.Sprintf(
		"/admin/v2/namespaces/%s/%s/%s/unload?destinationBroker=%s",
		tenant, namespace, bundleRange, dstTopicBrokerLookupURL)
	makeHTTPCall(t, http.MethodPut, greenAdminURL+unloadURL, "")
	wgUnload.Done()

	topicBrokerURL := ""
	retryAssert(t, 30, 2000, func() {
		lookupResult, _ := greenAdmin.Topics().Lookup(*topicName)
		topicBrokerURL = lookupResult.BrokerURL
	}, func(x assert.TestingT) bool {
		return req.Equal(dstTopicBrokerURL, topicBrokerURL)
	})

	wgRoutines.Wait()
}
