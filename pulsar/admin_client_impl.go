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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// AdminClient lets you interract with the Pulsar admin REST API
type AdminClient struct {
	client *http.Client

	// BaseURL is the base url for API requests
	BaseURL *url.URL

	// UserAgent to use when communicating with Pulsar API
	UserAgent string

	common service // Reuse a single struct instead of allocating one for each service on the heap.

	// Services used for talking to different parts of the Pulsar Admin API
	Tenants *TenantsService
}

func (c *AdminClient) NewRequest(method, urlStr string, body interface{}) (*http.Request, error) {
	if !strings.HasSuffix(c.BaseURL.Path, "/") {
		return nil, fmt.Errorf("BaseURL must have a trailing slash, but %q does not", c.BaseURL)
	}

	u, err := c.BaseURL.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	var buf io.ReadWriter
	if body != nil {
		buf = &bytes.Buffer{}
		enc := json.NewEncoder(buf)
		enc.SetEscapeHTML(false)
		err := enc.Encode(body)
		if err != nil {
			return nil, err
		}
	}

	req, err := http.NewRequest(method, u.String(), buf)
	if err != nil {
		return nil, err
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")

	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}
	return req, nil
}

// sanitizeURL redacts the client_secret parameter from the URL which may be
// exposed to the user.
func sanitizeURL(uri *url.URL) *url.URL {
	if uri == nil {
		return nil
	}
	params := uri.Query()
	if len(params.Get("client_secret")) > 0 {
		params.Set("client_secret", "REDACTED")
		uri.RawQuery = params.Encode()
	}
	return uri
}

// Do sends an API request and returns the API response. The API
// response is JSON decoded and stored in the value pointed to by
// v, or returned as an error if an API error has occurred. If v
// implements the io.Writer interface, the raw response body will
// be written to v, without attempting to first decode it.
//
// The provided ctx must be non-nil, if it is nil an error is
// returned. If it is canceled or times out, ctx.Err() will be
// returned.
func (c *AdminClient) Do(ctx context.Context, req *http.Request, v interface{}) (*http.Response, error) {
	if ctx == nil {
		return nil, errors.New("context must be non-nil")
	}
	req = withContext(ctx, req)

	resp, err := c.client.Do(req)
	if err != nil {
		// If we got an error, and the context has been canceled,
		// the context's error is probably more useful
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// If the error type is *url.Error, sanitize its URL before returning.
		if e, ok := err.(*url.Error); ok {
			if url, err := url.Parse(e.URL); err == nil {
				e.URL = sanitizeURL(url).String()
				return nil, e
			}
		}
	}
	defer resp.Body.Close()

	if err != nil {
		return resp, err
	}

	if v != nil {
		if w, ok := v.(io.Writer); ok {
			io.Copy(w, resp.Body)
		} else {
			decErr := json.NewDecoder(resp.Body).Decode(v)
			if decErr == io.EOF {
				decErr = nil // ignore EOF errors caused by empty response body
			}

			if decErr != nil {
				err = decErr
			}
		}
	}

	return resp, err
}

func withContext(ctx context.Context, req *http.Request) *http.Request {
	return req.WithContext(ctx)
}

type service struct {
	client *AdminClient
}

// BookiesService handles communication with bookie
// related methods of the Pulsar Admin API
//
// Pulsar API docs: https://pulsar.apache.org/admin-rest-api/#tag/bookies
type BookiesService service

// TenantsService handles communication with tenant related methods of the
// Pulsar Admin API
//
// Pulsar API docs: https://pulsar.apache.org/admin-rest-api/#tag/tenants
type TenantsService service

// GetTenants gets the list of existing tenants
//
// Pulsar API docs: https://pulsar.apache.org/admin-rest-api/#operation/getTenants
func (s *TenantsService) GetTenants(ctx context.Context) (*[]string, *http.Response, error) {
	u := "tenants"

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, nil, err
	}

	tenantList := new([]string)
	resp, err := s.client.Do(ctx, req, tenantList)
	if err != nil {
		return nil, resp, err
	}
	return tenantList, resp, nil
}
