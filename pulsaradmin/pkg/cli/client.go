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

package cli

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"

	"github.com/streamnative/pulsar-admin-go/pkg/auth"
)

// Client is a base client that is used to make http request to the ServiceURL
type Client struct {
	ServiceURL   string
	HTTPClient   *http.Client
	VersionInfo  string
	AuthProvider auth.Provider
}

func (c *Client) newRequest(method, path string) (*request, error) {
	base, _ := url.Parse(c.ServiceURL)
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

func (c *Client) doRequest(r *request) (*http.Response, error) {
	req, err := r.toHTTP()
	if err != nil {
		return nil, err
	}

	if r.contentType != "" {
		req.Header.Set("Content-Type", r.contentType)
	} else if req.Body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.useragent())

	if c.AuthProvider != nil {
		if c.AuthProvider.HasDataForHTTP() {
			headers, err := c.AuthProvider.GetHTTPHeaders()
			if err != nil {
				return nil, err
			}
			for k, v := range headers {
				req.Header.Set(k, v)
			}
		}
	}

	hc := c.HTTPClient
	if hc == nil {
		hc = http.DefaultClient
	}

	return hc.Do(req)
}

// MakeRequest can make a simple request and handle the response by yourself
func (c *Client) MakeRequest(method, endpoint string) (*http.Response, error) {
	req, err := c.newRequest(method, endpoint)
	if err != nil {
		return nil, err
	}

	resp, err := checkSuccessful(c.doRequest(req))
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *Client) Get(endpoint string, obj interface{}) error {
	_, err := c.GetWithQueryParams(endpoint, obj, nil, true)
	return err
}

func (c *Client) GetWithQueryParams(endpoint string, obj interface{}, params map[string]string,
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

func (c *Client) useragent() string {
	return c.VersionInfo
}

func (c *Client) Put(endpoint string, in interface{}) error {
	return c.PutWithQueryParams(endpoint, in, nil, nil)
}

func (c *Client) PutWithQueryParams(endpoint string, in, obj interface{}, params map[string]string) error {
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

func (c *Client) PutWithMultiPart(endpoint string, body io.Reader, contentType string) error {
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

func (c *Client) Delete(endpoint string) error {
	return c.DeleteWithQueryParams(endpoint, nil)
}

func (c *Client) DeleteWithQueryParams(endpoint string, params map[string]string) error {
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

	// nolint
	resp, err := checkSuccessful(c.doRequest(req))
	if err != nil {
		return err
	}
	defer safeRespClose(resp)

	return nil
}

func (c *Client) Post(endpoint string, in interface{}) error {
	return c.PostWithObj(endpoint, in, nil)
}

func (c *Client) PostWithObj(endpoint string, in, obj interface{}) error {
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

func (c *Client) PostWithMultiPart(endpoint string, in interface{}, body io.Reader, contentType string) error {
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

// responseError is used to parse a response into a client error
func responseError(resp *http.Response) error {
	var e Error
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		e.Reason = err.Error()
		e.Code = resp.StatusCode
		return e
	}

	err = json.Unmarshal(body, &e)
	if err != nil {
		e.Reason = string(body)
	}

	e.Code = resp.StatusCode

	if e.Reason == "" {
		e.Reason = unknownErrorReason
	}

	return e
}
