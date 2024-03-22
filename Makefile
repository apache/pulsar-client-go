#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

IMAGE_NAME = pulsar-client-go-test:latest
PULSAR_VERSION ?= 3.2.0
PULSAR_IMAGE = apachepulsar/pulsar:$(PULSAR_VERSION)
GO_VERSION ?= 1.18
CONTAINER_ARCH ?= $(shell uname -m)

# Golang standard bin directory.
GOPATH ?= $(shell go env GOPATH)
GOROOT ?= $(shell go env GOROOT)

build:
	go build ./pulsar
	go build ./pulsaradmin
	go build -o bin/pulsar-perf ./perf

lint: bin/golangci-lint
	bin/golangci-lint run

bin/golangci-lint:
	GOBIN=$(shell pwd)/bin go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.51.2

container:
	docker build -t ${IMAGE_NAME} \
	  --build-arg GO_VERSION="${GO_VERSION}" \
	  --build-arg PULSAR_IMAGE="${PULSAR_IMAGE}" \
	  --build-arg ARCH="${CONTAINER_ARCH}" .

test_extensible_load_manager: container
	PULSAR_VERSION=${PULSAR_VERSION} docker compose -f integration-tests/extensible-load-manager/docker-compose.yml up -d || true
	until curl http://localhost:8080/metrics > /dev/null 2>&1; do sleep 1; done
	# go test -race -timeout=1m -tags extensible_load_manager -run TestTopicUnloadWithAssignedUrl -v ./... || true
	docker run --network "extensible-load-manager_pulsar" -i ${IMAGE_NAME} bash -c "cd /pulsar/pulsar-client-go && ./scripts/run-ci-extensible-load-manager.sh"
	# go test -race -timeout=5m -tags extensible_load_manager -v -run TestExtensibleLoadManagerTestSuite ./...
	PULSAR_VERSION=${PULSAR_VERSION} docker compose -f integration-tests/extensible-load-manager/docker-compose.yml down

test: container
	docker run -i ${IMAGE_NAME} bash -c "cd /pulsar/pulsar-client-go && ./scripts/run-ci.sh"

clean:
	docker rmi --force $(IMAGE_NAME) || true
	rm bin/*
