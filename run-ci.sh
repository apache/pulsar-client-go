#!/bin/bash
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

set -e -x

export GOPATH=/

# Install dependencies
go mod download

# Basic compilation
go build ./pulsar
go build -o pulsar-perf ./perf

./pulsar-test-service-start.sh

go test -race -coverprofile=/tmp/coverage -timeout=1h ./...
go tool cover -html=/tmp/coverage -o coverage.html

## Check format
set +x # Hide all the subcommands outputs
GO_FILES=$(find . -name '*.go' | xargs)
FILES_WITH_BAD_FMT=$(gofmt -s -l $GO_FILES)

if [ -n "$FILES_WITH_BAD_FMT" ]
then
      echo "--- Invalid formatting on files: $FILES_WITH_BAD_FMT"
      echo "----------------------------------------------------"
      # Print diff
      gofmt -s -d $GO_FILES
      exit 1
fi

./pulsar-test-service-stop.sh

