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


set -e

SRC_DIR=$(git rev-parse --show-toplevel)
cd $SRC_DIR

IMAGE_NAME=pulsar-client-go-test:latest

if [[ -f /.dockerenv ]]; then
    # When running tests inside docker
    /pulsar/bin/pulsar-daemon stop standalone
else
    docker kill pulsar-client-go-test
fi

echo "Stopped Test Pulsar Service"
