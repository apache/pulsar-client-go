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

set -e

SRC_DIR=$(git rev-parse --show-toplevel)
cd $SRC_DIR

IMAGE_NAME=pulsar-client-go-test:latest

if [[ -f /.dockerenv ]]; then
    # When running tests inside docker
    PULSAR_ADMIN=/pulsar/bin/pulsar-admin
    /pulsar/bin/pulsar-daemon start standalone --no-functions-worker --no-stream-storage
else
    docker build -t ${IMAGE_NAME} .

    docker kill pulsar-client-go-test || true
    docker run -d --rm --name pulsar-client-go-test \
                -p 8080:8080 \
                -p 6650:6650 \
                -p 8443:8843 \
                -p 6651:6651 \
                ${IMAGE_NAME} \
                /pulsar/bin/pulsar standalone \
                    --no-functions-worker --no-stream-storage

    PULSAR_ADMIN="docker exec -it pulsar-client-go-test /pulsar/bin/pulsar-admin"
fi

echo "-- Wait for Pulsar service to be ready"
until curl http://localhost:8080/metrics > /dev/null 2>&1 ; do sleep 1; done

echo "-- Pulsar service is ready -- Configure permissions"

# Create "standalone" cluster
$PULSAR_ADMIN clusters create \
        standalone \
        --url http://localhost:8080/ \
        --url-secure https://localhost:8443/ \
        --broker-url pulsar://localhost:6650/ \
        --broker-url-secure pulsar+ssl://localhost:6651/

# Create "public" tenant
$PULSAR_ADMIN tenants create public -r "anonymous" -c "standalone"

# Create "public/default" with no auth required
$PULSAR_ADMIN namespaces create public/default
$PULSAR_ADMIN namespaces grant-permission public/default \
                        --actions produce,consume \
                        --role "anonymous"

# Create "private" tenant
$PULSAR_ADMIN tenants create private

# Create "private/auth" with required authentication
$PULSAR_ADMIN namespaces create private/auth
$PULSAR_ADMIN namespaces grant-permission private/auth \
                        --actions produce,consume \
                        --role "token-principal"

echo "-- Ready to start tests"
