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

package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"path"
)

type MediaType string

const (
	ApplicationJSON          MediaType = "application/json"
	PartitionedTopicMetaJSON MediaType = "application/vnd.partitioned-topic-metadata+json"
)

func (m MediaType) String() string {
	return string(m)
}

// Client is a base client that is used to make http request to the ServiceURL
type Client struct {
	ServiceURL  string
	HTTPClient  *http.Client
	VersionInfo string
}

func (c *Client) newRequest(method, path string) (*request, error) {
	base, err := url.Parse(c.ServiceURL)
	if err != nil {
		return nil, err
	}
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

func (c *Client) doRequest(ctx context.Context, r *request) (*http.Response, error) {
	req, err := r.toHTTP(ctx)
	if err != nil {
		return nil, err
	}

	if r.contentType != "" {
		req.Header.Set("Content-Type", r.contentType)
	} else if req.Body != nil {
		req.Header.Set("Content-Type", ApplicationJSON.String())
	}

	req.Header.Set("Accept", ApplicationJSON.String())
	req.Header.Set("User-Agent", c.useragent())
	hc := c.HTTPClient
	if hc == nil {
		hc = http.DefaultClient
	}

	return hc.Do(req)
}

// MakeRequest can make a simple request and handle the response by yourself
func (c *Client) MakeRequest(method, endpoint string) (*http.Response, error) {
	return c.MakeRequestWithContext(context.Background(), method, endpoint)
}

// MakeRequestWithContext can make a simple request and handle the response by yourself
func (c *Client) MakeRequestWithContext(ctx context.Context, method, endpoint string) (*http.Response, error) {
	req, err := c.newRequest(method, endpoint)
	if err != nil {
		return nil, err
	}

	resp, err := checkSuccessful(c.doRequest(ctx, req))
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *Client) MakeRequestWithURL(method string, urlOpt *url.URL) (*http.Response, error) {
	return c.MakeRequestWithURLWithContext(context.Background(), method, urlOpt)
}

func (c *Client) MakeRequestWithURLWithContext(
	ctx context.Context,
	method string,
	urlOpt *url.URL,
) (*http.Response, error) {
	req := &request{
		method: method,
		url:    urlOpt,
		params: make(url.Values),
	}
	resp, err := checkSuccessful(c.doRequest(ctx, req))
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *Client) Get(endpoint string, obj interface{}) error {
	return c.GetWithContext(context.Background(), endpoint, obj)
}

func (c *Client) GetWithContext(ctx context.Context, endpoint string, obj interface{}) error {
	_, err := c.GetWithQueryParamsWithContext(ctx, endpoint, obj, nil, true)
	return err
}

func (c *Client) GetBodyWithContext(ctx context.Context, endpoint string, obj interface{}) ([]byte, error) {
	return c.GetWithQueryParamsWithContext(ctx, endpoint, obj, nil, true)
}

func (c *Client) GetWithQueryParams(endpoint string, obj interface{}, params map[string]string,
	decode bool) ([]byte, error) {
	return c.GetWithQueryParamsWithContext(context.Background(), endpoint, obj, params, decode)
}

func (c *Client) GetWithQueryParamsWithContext(
	ctx context.Context,
	endpoint string,
	obj interface{},
	params map[string]string,
	decode bool,
) ([]byte, error) {
	return c.GetWithOptionsWithContext(ctx, endpoint, obj, params, decode, nil)
}

func (c *Client) GetWithOptions(endpoint string, obj interface{}, params map[string]string,
	decode bool, file io.Writer) ([]byte, error) {
	return c.GetWithOptionsWithContext(context.Background(), endpoint, obj, params, decode, file)
}

func (c *Client) GetWithOptionsWithContext(
	ctx context.Context,
	endpoint string,
	obj interface{},
	params map[string]string,
	decode bool, file io.Writer,
) ([]byte, error) {

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

	//nolint:bodyclose
	resp, err := checkSuccessful(c.doRequest(ctx, req))
	if err != nil {
		return nil, err
	}
	defer safeRespClose(resp)

	if obj != nil {
		body, err := decodeJSONWithBody(resp, &obj)
		if err != nil {
			if err == io.EOF {
				return nil, nil
			}
			return nil, err
		}
		return body, err
	} else if !decode {
		if file != nil {
			_, err := io.Copy(file, resp.Body)
			if err != nil {
				return nil, err
			}
		} else {
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return nil, err
			}
			return body, err
		}
	}

	return nil, err
}

func (c *Client) useragent() string {
	return c.VersionInfo
}

func (c *Client) Put(endpoint string, in interface{}) error {
	return c.PutWithContext(context.Background(), endpoint, in)
}

func (c *Client) PutWithContext(ctx context.Context, endpoint string, in interface{}) error {
	return c.PutWithQueryParamsWithContext(ctx, endpoint, in, nil, nil)
}

func (c *Client) PutWithQueryParams(endpoint string, in, obj interface{}, params map[string]string) error {
	return c.PutWithQueryParamsWithContext(context.Background(), endpoint, in, obj, params)
}

func (c *Client) PutWithQueryParamsWithContext(
	ctx context.Context,
	endpoint string,
	in,
	obj interface{},
	params map[string]string,
) error {
	return c.PutWithCustomMediaTypeWithContext(ctx, endpoint, in, obj, params, "")
}

func (c *Client) PutWithCustomMediaType(
	endpoint string,
	in, obj interface{},
	params map[string]string,
	mediaType MediaType,
) error {
	return c.PutWithCustomMediaTypeWithContext(context.Background(), endpoint, in, obj, params, mediaType)
}

func (c *Client) PutWithCustomMediaTypeWithContext(
	ctx context.Context,
	endpoint string,
	in,
	obj interface{},
	params map[string]string,
	mediaType MediaType,
) error {
	req, err := c.newRequest(http.MethodPut, endpoint)
	if err != nil {
		return err
	}
	if mediaType != "" {
		req.contentType = mediaType.String()
	}
	req.obj = in

	if params != nil {
		query := req.url.Query()
		for k, v := range params {
			query.Add(k, v)
		}
		req.params = query
	}

	//nolint:bodyclose
	resp, err := checkSuccessful(c.doRequest(ctx, req))
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

func (c *Client) PutWithMultiPart(
	endpoint string,
	body io.Reader,
	contentType string,
) error {
	return c.PutWithMultiPartWithContext(context.Background(), endpoint, body, contentType)
}

func (c *Client) PutWithMultiPartWithContext(
	ctx context.Context,
	endpoint string,
	body io.Reader,
	contentType string,
) error {
	req, err := c.newRequest(http.MethodPut, endpoint)
	if err != nil {
		return err
	}
	req.body = body
	req.contentType = contentType

	//nolint
	resp, err := checkSuccessful(c.doRequest(ctx, req))
	if err != nil {
		return err
	}
	defer safeRespClose(resp)

	return nil
}

func (c *Client) Delete(endpoint string) error {
	return c.DeleteWithContext(context.Background(), endpoint)
}

func (c *Client) DeleteWithContext(ctx context.Context, endpoint string) error {
	return c.DeleteWithQueryParamsWithContext(ctx, endpoint, nil)
}

func (c *Client) DeleteWithQueryParams(endpoint string, params map[string]string) error {
	return c.DeleteWithQueryParamsWithContext(context.Background(), endpoint, params)
}

func (c *Client) DeleteWithQueryParamsWithContext(
	ctx context.Context,
	endpoint string,
	params map[string]string,
) error {
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

	//nolint
	resp, err := checkSuccessful(c.doRequest(ctx, req))
	if err != nil {
		return err
	}
	defer safeRespClose(resp)

	return nil
}

func (c *Client) Post(endpoint string, in interface{}) error {
	return c.PostWithContext(context.Background(), endpoint, in)
}

func (c *Client) PostWithContext(ctx context.Context, endpoint string, in interface{}) error {
	return c.PostWithObjWithContext(ctx, endpoint, in, nil)
}

func (c *Client) PostWithObj(endpoint string, in, obj interface{}) error {
	return c.PostWithObjWithContext(context.Background(), endpoint, in, obj)
}

func (c *Client) PostWithObjWithContext(ctx context.Context, endpoint string, in, obj interface{}) error {
	req, err := c.newRequest(http.MethodPost, endpoint)
	if err != nil {
		return err
	}
	req.obj = in

	//nolint
	resp, err := checkSuccessful(c.doRequest(ctx, req))
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
	return c.PostWithMultiPartWithContext(context.Background(), endpoint, in, body, contentType)
}

func (c *Client) PostWithMultiPartWithContext(
	ctx context.Context,
	endpoint string,
	in interface{},
	body io.Reader,
	contentType string,
) error {
	req, err := c.newRequest(http.MethodPost, endpoint)
	if err != nil {
		return err
	}
	req.obj = in
	req.body = body
	req.contentType = contentType

	//nolint
	resp, err := checkSuccessful(c.doRequest(ctx, req))
	if err != nil {
		return err
	}
	defer safeRespClose(resp)

	return nil
}

func (c *Client) PostWithQueryParams(endpoint string, in interface{}, params map[string]string) error {
	return c.PostWithQueryParamsWithContext(context.Background(), endpoint, in, params)
}

func (c *Client) PostWithQueryParamsWithContext(
	ctx context.Context,
	endpoint string,
	in interface{},
	params map[string]string,
) error {
	req, err := c.newRequest(http.MethodPost, endpoint)
	if err != nil {
		return err
	}
	if in != nil {
		req.obj = in
	}
	if params != nil {
		query := req.url.Query()
		for k, v := range params {
			query.Add(k, v)
		}
		req.params = query
	}
	//nolint
	resp, err := checkSuccessful(c.doRequest(ctx, req))
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

func (r *request) toHTTP(ctx context.Context) (*http.Request, error) {
	r.url.RawQuery = r.params.Encode()

	// add a request body if there is one
	if r.body == nil && r.obj != nil {
		body, err := encodeJSONBody(r.obj)
		if err != nil {
			return nil, err
		}
		r.body = body
	}

	req, err := http.NewRequestWithContext(ctx, r.method, r.url.RequestURI(), r.body)
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
	b, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(b), nil
}

// decodeJSONBody is used to JSON decode a body
func decodeJSONBody(resp *http.Response, out interface{}) error {
	if resp.ContentLength == 0 {
		return nil
	}
	dec := json.NewDecoder(resp.Body)
	return dec.Decode(out)
}

// decodeJSONBody is used to JSON decode a body AND ALSO return the raw body bytes
func decodeJSONWithBody(resp *http.Response, out interface{}) ([]byte, error) {
	// Read the body first so we can return it even after decoding
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if len(body) == 0 {
		return nil, nil
	}

	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}

	return body, nil
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
	e := Error{
		Code:   resp.StatusCode,
		Reason: resp.Status,
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		e.Reason = err.Error()
		return e
	}

	err = json.Unmarshal(body, &e)
	if err != nil {
		if len(body) != 0 {
			e.Reason = string(body)
		}
		return e
	}

	return e
}
