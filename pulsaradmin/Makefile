# Copyright 2023 StreamNative, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

.PHONY: all
all: license-check lint test

.PHONY: lint
lint: golangci-lint
	$(GOLANGCI_LINT) run

.PHONY: test
test:
	@go build ./... && go test -race ./...

.PHONY: license-check
license-check: license-eye
	$(LICENSE_EYE) header check

.PHONY: license-fix
license-fix:
	$(LICENSE_EYE) header fix

# Install development dependencies

ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

LICENSE_EYE ?= $(GOBIN)/license-eye
GOLANGCI_LINT ?= $(GOBIN)/golangci-lint

.PHONY: license-eye
license-eye: $(LICENSE_EYE)
$(LICENSE_EYE): $(GOBIN)
	test -s $(GOBIN)/license-eye || go install github.com/apache/skywalking-eyes/cmd/license-eye@e1a0235

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT)
$(GOLANGCI_LINT): $(GOBIN)
	test -s $(GOBIN)/golangci-lint || go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.51.2
