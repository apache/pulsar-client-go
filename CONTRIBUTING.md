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

If you would like to contribute code to this project you can do so through GitHub by forking the repository and sending a pull request.

This document outlines some of the conventions on development workflow, commit message formatting, contact points and other resources to make it easier to get your contribution accepted.

## Steps to Contribute

Since the `go mod` package management tool is used in this project, your go version is required at **Go1.11+**.

### Fork

Before you start contributing, you need to fork [pulsar-client-go](https://github.com/apache/pulsar) to your github repository.

### Installation

If you don't currently have a go environment installed，install Go according to the installation instructions here: http://golang.org/doc/install

##### mac os && linux

```bash
$ mkdir -p $HOME/github.com/apache/
$ cd $HOME/github.com/apache/
$ git clone git@github.com:[your-github-id]/pulsar-client-go.git
$ cd pulsar-client-go
$ go mod download
```

When you execute `go mod download`, there may be some libs that cannot be downloaded. You can download them by referring to the proxy provided by [GOPROXY.io](https://goproxy.io/).

### Contribution flow

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

Thanks for your contributions!

#### Code style

The coding style suggested by the Golang community is used in Apache pulsar-client-go. See the [style doc](https://github.com/golang/go/wiki/CodeReviewComments) for details.

Please follow this style to make your pull request easy to review, maintain and develop.

#### Create new file

The project uses the open source protocol of Apache License 2.0. When you need to create a new file when developing new features, 
please add it at the beginning of the file. The location of the header file: [header file](.header).

#### Updating dependencies

Apache `pulsar-client-go` uses [Go 1.11 module](https://github.com/golang/go/wiki/Modules) to manage dependencies. To add or update a dependency: use the `go mod edit` command to change the dependency.
