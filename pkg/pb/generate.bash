#!/usr/bin/env bash
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

echo "generate pulsar go client protobuf code..."

pkg=pb

#if you have encountered the following error:
#   undefined: proto.ProtoPackageIsVersion3
#its because the protoc-gen-go's version not correct.
#
# $ git clone https://github.com/golang/protobuf
# $ cd ~/protobuf/protoc-gen-go
# $ git chectout tags/v1.2.0 -b v1.2.0
# $ go install

protoc --go_out=import_path=${pkg}:. PulsarApi.proto
goimports -w *.pb.go
