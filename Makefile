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
PULSAR_VERSION ?= 2.10.3
PULSAR_IMAGE = apachepulsar/pulsar:$(PULSAR_VERSION)
GO_VERSION ?= 1.18
GOLANG_IMAGE = golang:$(GO_VERSION)

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
	docker build -t ${IMAGE_NAME} --build-arg GOLANG_IMAGE="${GOLANG_IMAGE}" \
	    --build-arg PULSAR_IMAGE="${PULSAR_IMAGE}" .

test: container
	docker run -i ${IMAGE_NAME} bash -c "cd /pulsar/pulsar-client-go && ./scripts/run-ci.sh"

clean:
	docker rmi --force $(IMAGE_NAME) || true
	rm bin/*
