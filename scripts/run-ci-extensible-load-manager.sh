#!/bin/bash
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

set -x
set -o pipefail

TEST_LOG=/tmp/test-log-$(date +%s).log

# Default values for test configuration
: "${TEST_RACE:=1}"
: "${TEST_COVERAGE:=0}"

# Build the test command dynamically
TEST_CMD="go test"
if [ "$TEST_RACE" = "1" ]; then
    TEST_CMD="$TEST_CMD -race"
fi
if [ "$TEST_COVERAGE" = "1" ]; then
    TEST_CMD="$TEST_CMD -coverprofile=/tmp/coverage"
fi
TEST_CMD="$TEST_CMD -timeout=5m -tags extensible_load_manager -v -run TestExtensibleLoadManagerTestSuite ./pulsar"

$TEST_CMD 2>&1 | tee $TEST_LOG
retval=$?
if [ $retval -ne 0 ]; then
    # Make it easier to find out which test failed
    echo "Tests failed"
    grep -- "--- FAIL: " $TEST_LOG
    exit $retval
else
    echo "Tests passed"
    if [ "$TEST_COVERAGE" = "1" ]; then
        go tool cover -html=/tmp/coverage -o coverage.html
    fi
fi

