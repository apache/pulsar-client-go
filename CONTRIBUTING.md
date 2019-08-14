<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# How to contribute

If you would like to contribute code to this project, fork the repository and send a pull request.

## Prerequisite

If you have not installed Go, install it according to the [installation instruction](http://golang.org/doc/install).

Since the `go mod` package management tool is used in this project, **Go 1.11 or higher** version is required.

### Install Go on Mac OS and Linux

```bash
$ mkdir -p $HOME/github.com/apache/
$ cd $HOME/github.com/apache/
$ git clone git@github.com:[your-github-id]/pulsar-client-go.git
$ cd pulsar-client-go
$ go mod download
```

If some libs cannot be downloaded when you enter the `go mod download` command, download them by referring to the proxy provided by [GOPROXY.io](https://goproxy.io/).

## Fork

Before contributing, you need to fork [pulsar-client-go](https://github.com/apache/pulsar) to your github repository.

## Contribution flow

```bash
$ git remote add apache git@github.com:apache/pulsar-client-go.git
// sync with remote master
$ git checkout master
$ git fetch apache
$ git rebase apache/master
$ git push origin master
// create PR branch
$ git checkout -b your_branch   
# do your work, and then
$ git add [your change files]
$ git commit -sm "xxx"
$ git push origin your_branch
```

## Code style

The coding style suggested by the Golang community is used in Apache pulsar-client-go. For details, refer to [style doc](https://github.com/golang/go/wiki/CodeReviewComments).
Follow the style, make your pull request easy to review, maintain and develop.

## Create new files

The project uses the open source protocol of Apache License 2.0. If you need to create a new file when developing new features, 
add the license at the beginning of each file. The location of the header file: [header file](.header).

## Update dependencies

Apache `pulsar-client-go` uses [Go 1.11 module](https://github.com/golang/go/wiki/Modules) to manage dependencies. To add or update a dependency, use the `go mod edit` command to change the dependency.