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

PROJECT_DIR=$(git rev-parse --show-toplevel)
SOURCE_PATH="$PROJECT_DIR/integration-tests/pb"
DEST_PATH="$PROJECT_DIR/integration-tests/pb"
echo "source_path: $SOURCE_PATH"
echo "dest_path: $DEST_PATH"
protoc -I=$SOURCE_PATH --go_out=$DEST_PATH $SOURCE_PATH/*.proto
