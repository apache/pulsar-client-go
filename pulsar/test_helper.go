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
	"net/http"
	"path"
	"strings"
	"testing"
	"time"

	pkgerrors "github.com/pkg/errors"
)

const (
	serviceURL    = "pulsar://localhost:6650"
	serviceURLTLS = "pulsar+ssl://localhost:6651"

	webServiceURL = "http://localhost:8080"

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

func testEndpoint(parts ...string) string {
	return webServiceURL + "/" + path.Join(parts...)
}

func httpDelete(requestPaths ...string) error {
	client := http.DefaultClient
	var errs error
	doFn := func(requestPath string) error {
		endpoint := testEndpoint(requestPath)
		req, err := http.NewRequest(http.MethodDelete, endpoint, nil)
		if err != nil {
			return err
		}

		req.Header = map[string][]string{
			"Content-Type": {"application/json"},
		}

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		if resp.StatusCode > 299 {
			return fmt.Errorf("failed to delete topic status code: %d", resp.StatusCode)
		}
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
		return nil
	}
	for _, requestPath := range requestPaths {
		if err := doFn(requestPath); err != nil {
			err = pkgerrors.Wrapf(err, "unable to delete url: %s"+requestPath)
		}
	}
	return errs
}

func httpPut(requestPath string, body interface{}) error {
	client := http.DefaultClient

	data, _ := json.Marshal(body)
	endpoint := testEndpoint(requestPath)
	req, err := http.NewRequest(http.MethodPut, endpoint, bytes.NewReader(data))
	if err != nil {
		return err
	}

	req.Header = map[string][]string{
		"Content-Type": {"application/json"},
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.Body != nil {
		_ = resp.Body.Close()
	}
	return nil
}

func makeHTTPCall(t *testing.T, method string, url string, body string) {
	client := http.Client{}

	req, err := http.NewRequest(method, url, strings.NewReader(body))
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

func createNamespace(namespace string, policy map[string]interface{}) error {
	return httpPut("admin/v2/namespaces/"+namespace, policy)
}

func deleteNamespace(namespace string) error {
	return httpDelete("admin/v2/namespaces/" + namespace)
}

func createTopic(topic string) error {
	return httpPut("admin/v2/persistent/"+topic, nil)
}

func deleteTopic(topic string) error {
	return httpDelete("admin/v2/persistent/" + fmt.Sprintf("%s?force=true", topic))
}
