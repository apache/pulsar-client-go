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

package padmin

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"time"
)

type HttpClient interface {
	Get(path string) (*http.Response, error)
	Put(path string, body any) (*http.Response, error)
	Post(path string, body any) (*http.Response, error)
	Delete(path string) (*http.Response, error)
	Do(*http.Request) (*http.Response, error)
}

type HttpClientImpl struct {
	urlPrefix string
	cli       *http.Client
}

func (h *HttpClientImpl) Get(path string) (*http.Response, error) {
	url := h.urlPrefix + path
	return h.cli.Get(url)
}

func (h *HttpClientImpl) Put(path string, body any) (*http.Response, error) {
	url := h.urlPrefix + path
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	var putData io.Reader
	if body != nil {
		putData = bytes.NewBuffer(data)
	}
	req, err := http.NewRequest("PUT", url, putData)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return h.cli.Do(req)
}

func (h *HttpClientImpl) Delete(path string) (*http.Response, error) {
	url := h.urlPrefix + path
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return nil, err
	}
	return h.cli.Do(req)
}

func (h *HttpClientImpl) Post(path string, body any) (*http.Response, error) {
	url := h.urlPrefix + path
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return h.cli.Do(req)
}

func (h *HttpClientImpl) Do(req *http.Request) (*http.Response, error) {
	return h.cli.Do(req)
}

func newHttpClient(config Config) (HttpClient, error) {
	var cli *http.Client
	if config.TlsEnable {
		cli = &http.Client{
			Timeout: time.Millisecond * time.Duration(config.ConnectionTimeout),
			Transport: &http.Transport{
				TLSClientConfig: config.TlsConfig,
			},
		}
	} else {
		cli = &http.Client{
			Timeout: time.Millisecond * time.Duration(config.ConnectionTimeout),
		}
	}
	return &HttpClientImpl{
		urlPrefix: config.urlPrefix,
		cli:       cli,
	}, nil
}
