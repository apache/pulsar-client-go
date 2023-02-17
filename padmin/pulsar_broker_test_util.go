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

package padmin

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"net/http"
	"testing"
	"time"
)

type TestBroker struct {
	container testcontainers.Container
	webPort   int
	tcpPort   int
}

func (tb *TestBroker) Close() error {
	if tb.container != nil {
		return tb.container.Terminate(context.Background())
	}
	return nil
}

func startTestBroker(t *testing.T) *TestBroker {
	// you can also test with your local pulsar instance
	resp, err := http.Get("http://localhost:8080/admin/v2/brokers/health")
	if err != nil {
		return startTestBrokerDocker(t)
	}
	if resp.StatusCode != 200 {
		return startTestBrokerDocker(t)
	}
	return &TestBroker{
		webPort: 8080,
		tcpPort: 6650,
	}
}

func startTestBrokerDocker(t *testing.T) *TestBroker {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "apachepulsar/pulsar:latest",
		ExposedPorts: []string{"6650/tcp", "8080/tcp"},
		Cmd:          []string{"/pulsar/bin/pulsar", "standalone", "--no-functions-worker", "--no-stream-storage"},
		WaitingFor: wait.ForHTTP("/admin/v2/brokers/health").WithPort("8080/tcp").WithStatusCodeMatcher(func(statusCode int) bool {
			return statusCode == 200
		}).WithStartupTimeout(3 * time.Minute),
		AutoRemove: true,
	}
	tb := &TestBroker{}
	var err error
	tb.container, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.Nil(t, err)
	mapWebPort, err := tb.container.MappedPort(ctx, "8080/tcp")
	tb.webPort = mapWebPort.Int()
	require.Nil(t, err)
	mapTcpPort, err := tb.container.MappedPort(ctx, "6650/tcp")
	tb.tcpPort = mapTcpPort.Int()
	require.Nil(t, err)
	return tb
}
