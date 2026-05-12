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

package tests

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsaradmin"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin/config"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type pulsarTestContainer struct {
	testcontainers.Container
	BrokerURL string
	AdminURL  string
	Admin     admin.Client
}

var testPulsar *pulsarTestContainer

func TestMain(m *testing.M) {
	_ = os.Setenv("NO_PROXY", "localhost,127.0.0.1,0.0.0.0")
	_ = os.Setenv("no_proxy", "localhost,127.0.0.1,0.0.0.0")

	ctx := context.Background()
	var err error
	testPulsar, err = startPulsarContainer(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start Pulsar container: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()
	if err := testPulsar.Terminate(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "failed to terminate Pulsar container: %v\n", err)
		if code == 0 {
			code = 1
		}
	}
	os.Exit(code)
}

func startPulsarContainer(ctx context.Context) (*pulsarTestContainer, error) {
	req := testcontainers.ContainerRequest{
		Image:        pulsarTestImage(),
		ExposedPorts: []string{"6650/tcp", "8080/tcp"},
		WaitingFor: wait.ForHTTP("/admin/v2/clusters").
			WithPort("8080/tcp").
			WithResponseMatcher(func(body io.Reader) bool {
				respBytes, _ := io.ReadAll(body)
				return string(respBytes) == `["standalone"]`
			}).
			WithStartupTimeout(2 * time.Minute),
		Cmd: []string{"bin/pulsar", "standalone", "-nfw", "--advertised-address", "localhost"},
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	brokerURL, err := container.PortEndpoint(ctx, "6650", "pulsar")
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, err
	}
	adminURL, err := container.PortEndpoint(ctx, "8080", "http")
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, err
	}
	adminClient, err := pulsaradmin.NewClient(&config.Config{WebServiceURL: adminURL})
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, err
	}

	return &pulsarTestContainer{
		Container: container,
		BrokerURL: brokerURL,
		AdminURL:  adminURL,
		Admin:     adminClient,
	}, nil
}

func pulsarTestImage() string {
	image := os.Getenv("PULSAR_IMAGE")
	if image == "" {
		image = "apachepulsar/pulsar:latest"
	}
	return image
}
