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

package pulsar

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"testing"
	"time"
)

const (
	serviceURL    = "pulsar://localhost:6650"
	serviceURLTLS = "pulsar+ssl://localhost:6651"

	caCertsPath       = "../integration-tests/certs/cacert.pem"
	tlsClientCertPath = "../integration-tests/certs/client-cert.pem"
	tlsClientKeyPath  = "../integration-tests/certs/client-key.pem"
	tokenFilePath     = "../integration-tests/tokens/token.txt"
)

func newTopicName() string {
	return fmt.Sprintf("my-topic-%v", time.Now().Nanosecond())
}

func newAuthTopicName() string {
	return fmt.Sprintf("private/auth/my-topic-%v", time.Now().Nanosecond())
}

func httpPut(url string, body interface{}) {
	client := http.DefaultClient

	data, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		log.Fatal(err)
	}

	req.Header = map[string][]string{
		"Content-Type": {"application/json"},
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	if resp.Body != nil {
		_ = resp.Body.Close()
	}
}

func makeHTTPCall(t *testing.T, method string, urls string, body string) {
	client := http.Client{}

	req, err := http.NewRequest(method, urls, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if res.Body != nil {
		_ = res.Body.Close()
	}
}
