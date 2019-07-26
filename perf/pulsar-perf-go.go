//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package main

import (
	"github.com/spf13/cobra"

	log "github.com/sirupsen/logrus"
)

type ClientArgs struct {
	ServiceURL string
}

var clientArgs ClientArgs

func main() {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "15:04:05.000",
	})
	log.SetLevel(log.InfoLevel)

	initProducer()
	initConsumer()

	var rootCmd = &cobra.Command{Use: "pulsar-perf-go"}
	rootCmd.Flags().StringVarP(&clientArgs.ServiceURL, "service-url", "u",
		"pulsar://localhost:6650", "The Pulsar service URL")
	rootCmd.AddCommand(cmdProduce, cmdConsume)

	err := rootCmd.Execute()
	if err != nil {
		panic("execute root cmd error, please check.")
	}
}
