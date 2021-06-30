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

export CA_HOME=$(pwd)
echo $CA_HOME

if [ -d "certs" ]; then
  rm -rf certs
fi

if [ -d "crl" ]; then
  rm -rf crl
fi

if [ -d "newcerts" ]; then
  rm -rf newcerts
fi

if [ -d "private" ]; then
  rm -rf private
fi

if [ -d "index.txt" ]; then
  rm -rf index.txt
fi

if [ -d "serial" ]; then
  rm -rf serial
fi

mkdir certs crl newcerts private
chmod 700 private/
touch index.txt
echo 1000 > serial
openssl genrsa -out private/ca.key.pem 4096

openssl req -config openssl.cnf -key private/ca.key.pem \
    -new -x509 -days 7300 -sha256 -extensions v3_ca \
    -out certs/ca.cert.pem -subj '/C=US/ST=California/O=Apache Software Foundation/OU=Pulsar/CN=Pulsar CA/emailAddress=dev@pulsar.apache.org'

openssl genrsa -out private/broker.key.pem 2048
openssl pkcs8 -topk8 -inform PEM -outform PEM \
      -in private/broker.key.pem -out private/broker.key-pk8.pem -nocrypt
openssl req -config openssl.cnf \
    -key private/broker.key.pem -new -sha256 -out crl/broker.csr.pem -subj '/C=US/ST=California/O=Apache Software Foundation/OU=Pulsar/CN=localhost/emailAddress=dev@pulsar.apache.org'
yes | openssl ca -config openssl.cnf -extensions server_cert \
    -days 3650 -notext -md sha256 \
    -in crl/broker.csr.pem -out certs/broker.cert.pem

openssl genrsa -out private/client.key.pem 2048
openssl pkcs8 -topk8 -inform PEM -outform PEM \
      -in private/client.key.pem -out private/client.key-pk8.pem -nocrypt
openssl req -config openssl.cnf \
      -key private/client.key.pem -new -sha256 -out crl/client.csr.pem -subj '/C=US/ST=California/O=Apache Software Foundation/OU=Pulsar/CN=admin/emailAddress=dev@pulsar.apache.org'
yes | openssl ca -config openssl.cnf -extensions usr_cert \
      -days 3650 -notext -md sha256 \
      -in crl/client.csr.pem -out certs/client.cert.pem

mv certs/ca.cert.pem cacert.pem
mv certs/broker.cert.pem broker-cert.pem
mv private/broker.key-pk8.pem broker-key.pem
mv certs/client.cert.pem client-cert.pem
mv private/client.key-pk8.pem client-key.pem

rm -rf certs crl newcerts private index.txt* serial*