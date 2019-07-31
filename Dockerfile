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

FROM apachepulsar/pulsar:latest as pulsar

FROM golang:1.12 as go

# RUN apt-get update && apt-get install -y openjdk-8-jre-headless

COPY --from=pulsar /pulsar /pulsar

### Add test scripts

COPY integration-tests/certs /pulsar/certs
COPY integration-tests/tokens /pulsar/tokens
COPY integration-tests/standalone.conf /pulsar/conf
COPY integration-tests/client.conf /pulsar/conf
COPY pulsar-test-service-start.sh /pulsar/bin
COPY pulsar-test-service-stop.sh /pulsar/bin
COPY run-ci.sh /pulsar/bin
