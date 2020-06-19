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

package internal

import (
	"errors"
	"math/rand"
	"net/url"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// HostResolve is used to implement multihost
type HostResolve interface {
	GetServiceURL() string
	GetHost() (*url.URL, error)
	GetHostURL() (string, error)
	ResolveHost() (*url.URL, error)
}

type hostResolve struct {
	sync.Mutex

	serviceURL   string
	Host         []*url.URL
	CurrentIndex int
}

func NewHostResolve(serviceURL string) (HostResolve, error) {
	h := &hostResolve{
		serviceURL: serviceURL,
	}

	// resolve host
	hosts := strings.Split(h.serviceURL, ",")
	if len(hosts) == 0 || !checkPrefix(hosts[0]) {
		return nil, errors.New("invalid service URL")
	}

	for k := range hosts {
		if !checkPrefix(hosts[k]) {
			hosts[k] = h.Host[k-1].Scheme + `://` + hosts[k]
		}
		urlResolve, err := url.Parse(hosts[k])
		if err != nil {
			return nil, errors.New("invalid service URL")
		}
		h.Host = append(h.Host, urlResolve)
	}

	h.CurrentIndex = randomIndex(len(h.Host))
	return h, nil
}

func (h *hostResolve) GetServiceURL() string {
	return h.serviceURL
}

func (h *hostResolve) GetHostURL() (string, error) {
	host, err := h.GetHost()
	if err != nil {
		return "", err
	}
	return host.String(), nil
}

// add lock, to avoid race from ResolveHost()
func (h *hostResolve) GetHost() (*url.URL, error) {
	h.Lock()
	defer h.Unlock()

	if len(h.Host) == 0 || h.CurrentIndex > len(h.Host) {
		log.Debugf("h.host: %v, h.CurrentIndex: %d", h.Host, h.CurrentIndex)
		return nil, errors.New("not find host")
	}
	return h.Host[h.CurrentIndex], nil
}

func (h *hostResolve) ResolveHost() (*url.URL, error) {
	h.Lock()
	defer h.Unlock()

	if h.CurrentIndex >= len(h.Host) {
		return nil, errors.New("index out of range length of h.Host")
	}
	h.CurrentIndex = (h.CurrentIndex + 1) % len(h.Host)
	return h.Host[h.CurrentIndex], nil
}

func checkPrefix(url string) bool {
	if strings.HasPrefix(url, "pulsar") || strings.HasPrefix(url, "pulsar+ssl") {
		return true
	}

	return false
}

var r = &random{
	R: rand.New(rand.NewSource(time.Now().UnixNano())),
}

type random struct {
	sync.Mutex
	R *rand.Rand
}

func randomIndex(numHost int) int {
	r.Lock()
	defer r.Unlock()

	if numHost == 1 {
		return 0
	}
	return r.R.Intn(numHost)
}
