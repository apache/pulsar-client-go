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

set -e -x

if [ $# -ne 2 ]; then
    echo "Usage: $0 <version> <destination_directory>"
    exit 1
fi

VERSION=$1

DEST_PATH=$2
DEST_PATH="$(cd "$DEST_PATH" && pwd)"

pushd "$(dirname "$0")"
REPO_PATH=$(git rev-parse --show-toplevel)
popd

pushd "$REPO_PATH"
git archive --format=tar.gz --output="$DEST_PATH/apache-pulsar-client-go-$VERSION-src.tar.gz" HEAD
popd

# Sign all files
cd $DEST_PATH
gpg -b --armor apache-pulsar-client-go-$VERSION-src.tar.gz
shasum -a 512 apache-pulsar-client-go-$VERSION-src.tar.gz > apache-pulsar-client-go-$VERSION-src.tar.gz.sha512

