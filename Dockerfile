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

RUN cd /

RUN wget --no-cookies --no-check-certificate \
    --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u141-b15/336fa29ff2bb4ef291e347e091f7f4a7/jdk-8u141-linux-x64.tar.gz"
# make a new directory to store the jdk files
RUN mkdir -p /usr/local/java
RUN tar zxvf jdk-8u141-linux-x64.tar.gz
RUN mv jdk1.8.0_141 /usr/local/java/
# make a symbol link
RUN ln -s /usr/local/java/jdk1.8.0_141 /usr/local/java/jdk

# set environment variables
ENV JAVA_HOME /usr/local/java/jdk
ENV JRE_HOME ${JAVA_HOME}/jre
ENV CLASSPATH .:${JAVA_HOME}/lib:${JRE_HOME}/lib
ENV PATH ${JAVA_HOME}/bin:$PATH

RUN cd /go

COPY --from=pulsar /pulsar /pulsar

### Add test scripts

COPY integration-tests/certs /pulsar/certs
COPY integration-tests/tokens /pulsar/tokens
COPY integration-tests/standalone.conf /pulsar/conf
COPY integration-tests/client.conf /pulsar/conf
COPY pulsar-test-service-start.sh /pulsar/bin
COPY pulsar-test-service-stop.sh /pulsar/bin
COPY run-ci.sh /pulsar/bin
