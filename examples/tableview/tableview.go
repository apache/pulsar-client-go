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

package main

import (
	"fmt"
	"log"
	"reflect"

	"github.com/apache/pulsar-client-go/pulsar"
)

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	schema := pulsar.NewStringSchema(nil)
	v := ""

	tv, err := client.CreateTableView(pulsar.TableViewOptions{
		Topic:           "topic-1",
		Schema:          schema,
		SchemaValueType: reflect.TypeOf(&v),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer tv.Close()

	tv.ForEachAndListen(func(t string, data interface{}) error {
		fmt.Printf("receive tableview message, topic:%s data:%s \n", t, *data.(*string))
		return nil
	})

	select {}

}
