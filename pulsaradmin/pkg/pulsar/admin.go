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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/streamnative/pulsar-admin-go/pkg/auth"
)

const (
	DefaultWebServiceURL = "http://localhost:8080"
)

// Config is used to configure the admin client
type Config struct {
	WebServiceUrl string
	HttpClient    *http.Client
	ApiVersion    ApiVersion

	Auth       *auth.TlsAuthProvider
	AuthParams string
	TlsOptions *TLSOptions
}

type TLSOptions struct {
	TrustCertsFilePath      string
	AllowInsecureConnection bool
}

// DefaultConfig returns a default configuration for the pulsar admin client
func DefaultConfig() *Config {
	config := &Config{
		WebServiceUrl: DefaultWebServiceURL,
		HttpClient:    http.DefaultClient,

		TlsOptions: &TLSOptions{
			AllowInsecureConnection: false,
		},
	}
	return config
}

// Client provides a client to the Pulsar Restful API
type Client interface {
	Clusters() Clusters
	Functions() Functions
	Tenants() Tenants
	Topics() Topics
	Sources() Sources
	Sinks() Sinks
	Namespaces() Namespaces
	Schemas() Schema
}

type client struct {
	webServiceUrl string
	apiVersion    string
	httpClient    *http.Client

	// TLS config
	auth       *auth.TlsAuthProvider
	authParams string
	tlsOptions *TLSOptions
	transport  *http.Transport
}

// New returns a new client
func New(config *Config) (Client, error) {
	if len(config.WebServiceUrl) == 0 {
		config.WebServiceUrl = DefaultWebServiceURL
	}

	c := &client{
		apiVersion:    config.ApiVersion.String(),
		webServiceUrl: config.WebServiceUrl,
	}

	if strings.HasPrefix(c.webServiceUrl, "https://") {
		c.authParams = config.AuthParams
		c.tlsOptions = config.TlsOptions
		mapAuthParams := make(map[string]string)

		err := json.Unmarshal([]byte(c.authParams), &mapAuthParams)
		if err != nil {
			return nil, err
		}
		c.auth = auth.NewAuthenticationTLSWithParams(mapAuthParams)

		tlsConf, err := c.getTLSConfig()
		if err != nil {
			return nil, err
		}

		c.transport = &http.Transport{
			MaxIdleConnsPerHost: 10,
			TLSClientConfig:     tlsConf,
		}
	}

	return c, nil
}

func (c *client) getTLSConfig() (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: c.tlsOptions.AllowInsecureConnection,
	}

	if c.tlsOptions.TrustCertsFilePath != "" {
		caCerts, err := ioutil.ReadFile(c.tlsOptions.TrustCertsFilePath)
		if err != nil {
			return nil, err
		}

		tlsConfig.RootCAs = x509.NewCertPool()
		ok := tlsConfig.RootCAs.AppendCertsFromPEM(caCerts)
		if !ok {
			return nil, errors.New("failed to parse root CAs certificates")
		}
	}

	cert, err := c.auth.GetTLSCertificate()
	if err != nil {
		return nil, err
	}

	if cert != nil {
		tlsConfig.Certificates = []tls.Certificate{*cert}
	}

	return tlsConfig, nil
}

func (c *client) endpoint(componentPath string, parts ...string) string {
	return path.Join(makeHttpPath(c.apiVersion, componentPath), endpoint(parts...))
}

// get is used to do a GET request against an endpoint
// and deserialize the response into an interface

func (c *client) getWithQueryParams(endpoint string, obj interface{}, params map[string]string, decode bool) ([]byte, error) {

	req, err := c.newRequest(http.MethodGet, endpoint)
	if err != nil {
		return nil, err
	}

	if params != nil {
		query := req.url.Query()
		for k, v := range params {
			query.Add(k, v)
		}
		req.params = query
	}

	resp, err := checkSuccessful(c.doRequest(req))
	if err != nil {
		return nil, err
	}
	defer safeRespClose(resp)

	if obj != nil {
		if err := decodeJsonBody(resp, &obj); err != nil {
			return nil, err
		}
	} else if !decode {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return body, err
	}

	return nil, err
}

func (c *client) get(endpoint string, obj interface{}) error {
	_, err := c.getWithQueryParams(endpoint, obj, nil, true)
	return err
}

func (c *client) put(endpoint string, in, obj interface{}) error {
	return c.putWithQueryParams(endpoint, in, obj, nil)
}

func (c *client) putWithQueryParams(endpoint string, in, obj interface{}, params map[string]string) error {
	req, err := c.newRequest(http.MethodPut, endpoint)
	if err != nil {
		return err
	}
	req.obj = in

	if params != nil {
		query := req.url.Query()
		for k, v := range params {
			query.Add(k, v)
		}
		req.params = query
	}

	resp, err := checkSuccessful(c.doRequest(req))
	if err != nil {
		return err
	}
	defer safeRespClose(resp)

	if obj != nil {
		if err := decodeJsonBody(resp, &obj); err != nil {
			return err
		}
	}

	return nil
}

func (c *client) delete(endpoint string, obj interface{}) error {
	return c.deleteWithQueryParams(endpoint, obj, nil)
}

func (c *client) deleteWithQueryParams(endpoint string, obj interface{}, params map[string]string) error {
	req, err := c.newRequest(http.MethodDelete, endpoint)
	if err != nil {
		return err
	}

	if params != nil {
		query := req.url.Query()
		for k, v := range params {
			query.Add(k, v)
		}
		req.params = query
	}

	resp, err := checkSuccessful(c.doRequest(req))
	if err != nil {
		return err
	}
	defer safeRespClose(resp)

	if obj != nil {
		if err := decodeJsonBody(resp, &obj); err != nil {
			return err
		}
	}

	return nil
}

func (c *client) post(endpoint string, in, obj interface{}) error {
	req, err := c.newRequest(http.MethodPost, endpoint)
	if err != nil {
		return err
	}
	req.obj = in
	resp, err := checkSuccessful(c.doRequest(req))
	if err != nil {
		return err
	}
	defer safeRespClose(resp)
	if obj != nil {
		if err := decodeJsonBody(resp, &obj); err != nil {
			return err
		}
	}
	return nil
}

func (c *client) putWithMultiPart(endpoint string, in, obj interface{}, body io.Reader, contentType string) error {
	req, err := c.newRequest(http.MethodPut, endpoint)
	if err != nil {
		return err
	}
	req.obj = in
	req.body = body
	req.contentType = contentType

	resp, err := checkSuccessful(c.doRequest(req))
	if err != nil {
		return err
	}
	defer safeRespClose(resp)

	if obj != nil {
		if err := decodeJsonBody(resp, &obj); err != nil {
			return err
		}
	}

	return nil
}

func (c *client) postWithMultiPart(endpoint string, in, obj interface{}, body io.Reader, contentType string) error {
	req, err := c.newRequest(http.MethodPost, endpoint)
	if err != nil {
		return err
	}
	req.obj = in
	req.body = body
	req.contentType = contentType

	resp, err := checkSuccessful(c.doRequest(req))
	if err != nil {
		return err
	}
	defer safeRespClose(resp)

	if obj != nil {
		if err := decodeJsonBody(resp, &obj); err != nil {
			return err
		}
	}

	return nil
}

type request struct {
	method      string
	contentType string
	url         *url.URL
	params      url.Values

	obj  interface{}
	body io.Reader
}

func (r *request) toHTTP() (*http.Request, error) {
	r.url.RawQuery = r.params.Encode()

	// add a request body if there is one
	if r.body == nil && r.obj != nil {
		body, err := encodeJsonBody(r.obj)
		if err != nil {
			return nil, err
		}
		r.body = body
	}

	req, err := http.NewRequest(r.method, r.url.RequestURI(), r.body)
	if err != nil {
		return nil, err
	}

	req.URL.Host = r.url.Host
	req.URL.Scheme = r.url.Scheme
	req.Host = r.url.Host
	return req, nil
}

func (c *client) newRequest(method, path string) (*request, error) {
	base, _ := url.Parse(c.webServiceUrl)
	u, err := url.Parse(path)
	if err != nil {
		return nil, err
	}

	req := &request{
		method: method,
		url: &url.URL{
			Scheme: base.Scheme,
			User:   base.User,
			Host:   base.Host,
			Path:   endpoint(base.Path, u.Path),
		},
		params: make(url.Values),
	}
	return req, nil
}

// TODO: add pulsarctl version
func (c *client) useragent() string {
	return fmt.Sprintf("pulsarctl (go)")
}

func (c *client) doRequest(r *request) (*http.Response, error) {
	req, err := r.toHTTP()
	if err != nil {
		return nil, err
	}

	if r.contentType != "" {
		req.Header.Set("Content-Type", r.contentType)
	} else {
		// add default headers
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
	}

	req.Header.Set("User-Agent", c.useragent())

	hc := c.httpClient
	if hc == nil {
		hc = http.DefaultClient
	}
	if c.transport != nil {
		hc.Transport = c.transport
	}

	return hc.Do(req)
}

// decodeJsonBody is used to JSON encode a body
func encodeJsonBody(obj interface{}) (io.Reader, error) {
	buf := bytes.NewBuffer(nil)
	enc := json.NewEncoder(buf)
	if err := enc.Encode(obj); err != nil {
		return nil, err
	}
	return buf, nil
}

// decodeJsonBody is used to JSON decode a body
func decodeJsonBody(resp *http.Response, out interface{}) error {
	dec := json.NewDecoder(resp.Body)
	return dec.Decode(out)
}

// safeRespClose is used to close a respone body
func safeRespClose(resp *http.Response) {
	if resp != nil {
		if err := resp.Body.Close(); err != nil {
			// ignore error since it is closing a response body
		}
	}
}

// responseError is used to parse a response into a pulsar error
func responseError(resp *http.Response) error {
	var e Error
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		e.Reason = err.Error()
		e.Code = resp.StatusCode
		return e
	}

	json.Unmarshal(body, &e)

	e.Code = resp.StatusCode
	if e.Reason == "" {
		e.Reason = unknownErrorReason
	}

	return e
}

// respIsOk is used to validate a successful http status code
func respIsOk(resp *http.Response) bool {
	return resp.StatusCode >= http.StatusOK && resp.StatusCode <= http.StatusNoContent
}

// checkSuccessful checks for a valid response and parses an error
func checkSuccessful(resp *http.Response, err error) (*http.Response, error) {
	if err != nil {
		safeRespClose(resp)
		return nil, err
	}

	if !respIsOk(resp) {
		defer safeRespClose(resp)
		return nil, responseError(resp)
	}

	return resp, nil
}

func endpoint(parts ...string) string {
	return path.Join(parts...)
}
