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

#
# build pulsar-perf for multiple OS and Architecture
#

# absolute directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

GZ='false'

function help() {
  echo "usage: [-gz] | [-v <tag> | HEAD] | -h"
}

while [[ $# -gt 0 ]]
do
key="$1"

  case $key in
    -v|--version)
    TAG=$2
    if [ ${TAG} == "HEAD" ]; then
      TAG="$( git rev-parse --short HEAD )"
    fi
    TAG=-${TAG}
    shift # past argument
    shift # past value
    ;;
    -gz|--gzip)
    GZ='true'
    shift
    ;;
    -h|--help)
    help
    exit 2
    ;;
    *)    # unknown option
    shift
    ;;
  esac
done

function build_os_arch() {
  OS=$1
  ARCH=$2
  echo "build for OS $1 ARCH $2 version${TAG}"
  GOOS=$OS GOARCH=$ARCH go build -o ${DIR}/bin/releases/pulsar-perf$TAG-$OS-$ARCH ${DIR}/perf
  if [ ${GZ} == 'true' ]; then
    gzip ${DIR}/bin/releases/pulsar-perf$TAG-$OS-$ARCH
  fi
}
cd $DIR

mkdir -p ${DIR}/bin/releases
rm -f ${DIR}/bin/releases/pulsar-perf*

build_os_arch "linux" "amd64"
build_os_arch "linux" "arm"
build_os_arch "linux" "arm64"
build_os_arch "darwin" "amd64"
build_os_arch "windows" "amd64"
build_os_arch "windows" "arm"
build_os_arch "windows" "386"
build_os_arch "solaris" "amd64"
build_os_arch "freebsd" "amd64"
build_os_arch "freebsd" "arm64"
build_os_arch "openbsd" "amd64"
build_os_arch "openbsd" "arm64"

# end