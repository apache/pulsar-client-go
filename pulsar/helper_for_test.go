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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	"github.com/stretchr/testify/assert"

	pkgerrors "github.com/pkg/errors"
)

const (
	serviceURL    = "pulsar://localhost:6650"
	serviceURLTLS = "pulsar+ssl://localhost:6651"

	webServiceURL    = "http://localhost:8080"
	webServiceURLTLS = "https://localhost:8443"

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

func jsonHeaders() http.Header {
	headers := http.Header{}
	headers.Add("Content-Type", "application/json")
	headers.Add("Accept", "application/json")
	return headers
}

func httpDelete(requestPaths ...string) error {
	var errs error
	for _, requestPath := range requestPaths {
		if err := httpDo(http.MethodDelete, requestPath, nil, nil); err != nil {
			errs = pkgerrors.Wrapf(err, "unable to delete url: %s", requestPath)
		}
	}
	return errs
}

func httpPut(requestPath string, body interface{}) error {
	return httpDo(http.MethodPut, requestPath, body, nil)
}

func httpGet(requestPath string, out interface{}) error {
	return httpDo(http.MethodGet, requestPath, nil, out)
}

func httpDo(method string, requestPath string, in interface{}, out interface{}) error {
	client := http.DefaultClient
	endpoint := testEndpoint(requestPath)
	var body io.Reader
	inBytes, err := json.Marshal(in)
	if err != nil {
		return err
	}
	body = bytes.NewReader(inBytes)
	req, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		return err
	}

	req.Header = jsonHeaders()
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode > 299 || resp.StatusCode < 200 {
		return fmt.Errorf("http error status code: %d", resp.StatusCode)
	}

	if out != nil {
		outBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return json.Unmarshal(outBytes, out)
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

func createTopic(topic string) error {
	return httpPut("admin/v2/persistent/"+topic, nil)
}

func deleteTopic(topic string) error {
	return httpDelete("admin/v2/persistent/" + fmt.Sprintf("%s?force=true", topic))
}

func topicStats(topic string) (map[string]interface{}, error) {
	var metadata map[string]interface{}
	err := httpGet("admin/v2/persistent/"+topicPath(topic)+"/stats", &metadata)
	return metadata, err
}

// transactionStats queries the transaction metadata admin endpoint. Since
// apache/pulsar#26773 that endpoint requires super-user access, so the request must be
// authenticated. The plain-text httpGet helper sends no credentials and would be rejected
// as the "anonymous" role (HTTP 401), so use the TLS web service with the test client
// certificate instead, whose CN ("admin") is a super-user role in standalone.conf.
func transactionStats(id *TxnID) (map[string]interface{}, error) {
	var metadata map[string]interface{}
	path := fmt.Sprintf("admin/v3/transactions/transactionMetadata/%d/%d", id.MostSigBits, id.LeastSigBits)
	err := httpGetWithTLS(path, &metadata)
	return metadata, err
}

// httpGetWithTLS performs an HTTPS GET against the broker's web service, authenticating
// with the test client certificate. It is used for admin endpoints that require a
// super-user role.
func httpGetWithTLS(requestPath string, out interface{}) error {
	client, err := getTLSWebClient()
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodGet, webServiceURLTLS+"/"+requestPath, nil)
	if err != nil {
		return err
	}
	req.Header = jsonHeaders()
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode > 299 || resp.StatusCode < 200 {
		return fmt.Errorf("http error status code: %d", resp.StatusCode)
	}
	if out == nil {
		return nil
	}
	outBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(outBytes, out)
}

var (
	tlsWebClientOnce sync.Once
	tlsWebClient     *http.Client
	tlsWebClientErr  error
)

// getTLSWebClient lazily builds and caches an HTTPS client that authenticates with the
// test client certificate. It is used for admin endpoints that require a super-user role.
func getTLSWebClient() (*http.Client, error) {
	tlsWebClientOnce.Do(func() {
		cert, err := tls.LoadX509KeyPair(tlsClientCertPath, tlsClientKeyPath)
		if err != nil {
			tlsWebClientErr = err
			return
		}
		caCert, err := os.ReadFile(caCertsPath)
		if err != nil {
			tlsWebClientErr = err
			return
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			tlsWebClientErr = fmt.Errorf("failed to parse CA certificates from %s", caCertsPath)
			return
		}
		tlsWebClient = &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					Certificates: []tls.Certificate{cert},
					RootCAs:      pool,
				},
			},
		}
	})
	return tlsWebClient, tlsWebClientErr
}

func topicPath(topic string) string {
	tn, _ := internal.ParseTopicName(topic)
	idx := strings.LastIndex(tn.Name, "/")
	if idx > 0 {
		return tn.Namespace + "/" + tn.Name[idx:]
	}
	return tn.Name
}

func retryAssert(t assert.TestingT, times int, milliseconds int, update func(), assert func(assert.TestingT) bool) {
	for i := 0; i < times; i++ {
		time.Sleep(time.Duration(milliseconds) * time.Millisecond)
		update()
		if assert(fakeAssertT{}) {
			break
		}
	}
	assert(t)
}

type fakeAssertT struct{}

func (fa fakeAssertT) Errorf(_ string, _ ...interface{}) {
}
