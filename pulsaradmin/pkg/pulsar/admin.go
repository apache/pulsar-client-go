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
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/streamnative/pulsar-admin-go/pkg/auth"
)

const (
	DefaultWebServiceURL       = "http://localhost:8080"
	DefaultHTTPTimeOutDuration = 5 * time.Minute
)

var ReleaseVersion = "None"

// Config is used to configure the admin client
type Config struct {
	WebServiceURL string
	HTTPTimeout   time.Duration
	HTTPClient    *http.Client
	APIVersion    APIVersion

	Auth       *auth.TLSAuthProvider
	AuthParams string
	TLSOptions *TLSOptions
	TokenAuth  *auth.TokenAuthProvider
}

type TLSOptions struct {
	TrustCertsFilePath      string
	AllowInsecureConnection bool
}

// DefaultConfig returns a default configuration for the pulsar admin client
func DefaultConfig() *Config {
	config := &Config{
		WebServiceURL: DefaultWebServiceURL,
		HTTPClient: &http.Client{
			Timeout: DefaultHTTPTimeOutDuration,
		},

		TLSOptions: &TLSOptions{
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
	Subscriptions() Subscriptions
	Sources() Sources
	Sinks() Sinks
	Namespaces() Namespaces
	Schemas() Schema
	NsIsolationPolicy() NsIsolationPolicy
	Brokers() Brokers
	BrokerStats() BrokerStats
}

type client struct {
	webServiceURL string
	apiVersion    string
	httpClient    *http.Client
	versionInfo   string

	// TLS config
	auth       *auth.TLSAuthProvider
	authParams string
	tlsOptions *TLSOptions
	transport  *http.Transport

	tokenAuth *auth.TokenAuthProvider
}

// New returns a new client
func New(config *Config) (Client, error) {
	if len(config.WebServiceURL) == 0 {
		config.WebServiceURL = DefaultWebServiceURL
	}

	c := &client{
		apiVersion:    config.APIVersion.String(),
		webServiceURL: config.WebServiceURL,
		versionInfo:   ReleaseVersion,
		tokenAuth:     config.TokenAuth,
	}

	if strings.HasPrefix(c.webServiceURL, "https://") {
		c.authParams = config.AuthParams
		c.tlsOptions = config.TLSOptions
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
			TLSHandshakeTimeout: 15 * time.Second,
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
	return path.Join(makeHTTPPath(c.apiVersion, componentPath), endpoint(parts...))
}

// get is used to do a GET request against an endpoint
// and deserialize the response into an interface

func (c *client) getWithQueryParams(endpoint string, obj interface{}, params map[string]string,
	decode bool) ([]byte, error) {

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
		if err := decodeJSONBody(resp, &obj); err != nil {
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

func (c *client) put(endpoint string, in interface{}) error {
	return c.putWithQueryParams(endpoint, in, nil, nil)
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
		if err := decodeJSONBody(resp, &obj); err != nil {
			return err
		}
	}

	return nil
}

func (c *client) delete(endpoint string) error {
	return c.deleteWithQueryParams(endpoint, nil, nil)
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
		if err := decodeJSONBody(resp, &obj); err != nil {
			return err
		}
	}

	return nil
}

func (c *client) post(endpoint string, in interface{}) error {
	return c.postWithObj(endpoint, in, nil)
}

func (c *client) postWithObj(endpoint string, in, obj interface{}) error {
	req, err := c.newRequest(http.MethodPost, endpoint)
	if err != nil {
		return err
	}
	req.obj = in

	// nolint
	resp, err := checkSuccessful(c.doRequest(req))
	if err != nil {
		return err
	}
	defer safeRespClose(resp)
	if obj != nil {
		if err := decodeJSONBody(resp, &obj); err != nil {
			return err
		}
	}

	return nil
}

func (c *client) putWithMultiPart(endpoint string, body io.Reader, contentType string) error {
	req, err := c.newRequest(http.MethodPut, endpoint)
	if err != nil {
		return err
	}
	req.body = body
	req.contentType = contentType

	// nolint
	resp, err := checkSuccessful(c.doRequest(req))
	if err != nil {
		return err
	}
	defer safeRespClose(resp)

	return nil
}

func (c *client) postWithMultiPart(endpoint string, in interface{}, body io.Reader, contentType string) error {
	req, err := c.newRequest(http.MethodPost, endpoint)
	if err != nil {
		return err
	}
	req.obj = in
	req.body = body
	req.contentType = contentType

	// nolint
	resp, err := checkSuccessful(c.doRequest(req))
	if err != nil {
		return err
	}
	defer safeRespClose(resp)

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
		body, err := encodeJSONBody(r.obj)
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
	base, _ := url.Parse(c.webServiceURL)
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

func (c *client) useragent() string {
	return c.versionInfo
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

	if c.tokenAuth != nil {
		data, _ := c.tokenAuth.GetData()
		req.Header.Set("Authorization", "Bearer "+string(data))
	}

	req.Header.Set("User-Agent", c.useragent())

	hc := c.httpClient
	if hc == nil {
		hc = &http.Client{
			Timeout: DefaultHTTPTimeOutDuration,
		}
	}

	if c.transport != nil {
		hc.Transport = c.transport
	}

	return hc.Do(req)
}

// encodeJSONBody is used to JSON encode a body
func encodeJSONBody(obj interface{}) (io.Reader, error) {
	buf := bytes.NewBuffer(nil)
	enc := json.NewEncoder(buf)
	if err := enc.Encode(obj); err != nil {
		return nil, err
	}
	return buf, nil
}

// decodeJSONBody is used to JSON decode a body
func decodeJSONBody(resp *http.Response, out interface{}) error {
	dec := json.NewDecoder(resp.Body)
	return dec.Decode(out)
}

// safeRespClose is used to close a response body
func safeRespClose(resp *http.Response) {
	if resp != nil {
		// ignore error since it is closing a response body
		_ = resp.Body.Close()
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
