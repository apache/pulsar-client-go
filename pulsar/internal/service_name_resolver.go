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
	"fmt"
	"math/rand"
	"net/url"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type ServiceNameResolver interface {
	ResolveHost() (*url.URL, error)
	ResolveHostURI() (*PulsarServiceURI, error)
	UpdateServiceURL(serviceURL string) error
	GetServiceURI() *PulsarServiceURI
	GetAddressList() []*url.URL
}

type pulsarServiceNameResolver struct {
	ServiceURI   *PulsarServiceURI
	CurrentIndex int32
	AddressList  []*url.URL

	rnd   *rand.Rand
	mutex sync.Mutex
}

func NewPulsarServiceNameResolver(serviceURL string) (ServiceNameResolver, error) {
	r := &pulsarServiceNameResolver{rnd: rand.New(rand.NewSource(time.Now().UnixNano()))}
	if len(serviceURL) > 0 {
		return r, r.UpdateServiceURL(serviceURL)
	}
	return r, nil
}

func (r *pulsarServiceNameResolver) ResolveHost() (*url.URL, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.AddressList == nil {
		return nil, errors.New("no service url is provided yet")
	}
	if len(r.AddressList) == 0 {
		return nil, fmt.Errorf("no hosts found for service url : %v", r.ServiceURI)
	}
	if len(r.AddressList) == 1 {
		return r.AddressList[0], nil
	}
	idx := (r.CurrentIndex + 1) % int32(len(r.AddressList))
	r.CurrentIndex = idx
	return r.AddressList[idx], nil
}

func (r *pulsarServiceNameResolver) ResolveHostURI() (*PulsarServiceURI, error) {
	host, err := r.ResolveHost()
	if err != nil {
		return nil, err
	}
	hostURL := host.Scheme + "://" + host.Hostname() + ":" + host.Port()
	return NewPulsarServiceURIFromURIString(hostURL)
}

func (r *pulsarServiceNameResolver) UpdateServiceURL(serviceURL string) error {
	uri, err := NewPulsarServiceURIFromURIString(serviceURL)
	if err != nil {
		log.Errorf("invalid service-url instance %s provided %v", serviceURL, err)
		return err
	}

	hosts := uri.ServiceHosts
	addresses := []*url.URL{}
	for _, host := range hosts {
		hostURL := uri.URL.Scheme + "://" + host
		u, err := url.Parse(hostURL)
		if err != nil {
			log.Errorf("invalid host-url %s provided %v", hostURL, err)
			return err
		}
		addresses = append(addresses, u)
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.AddressList = addresses
	r.ServiceURI = uri
	r.CurrentIndex = int32(r.rnd.Intn(len(addresses)))
	return nil
}

func (r *pulsarServiceNameResolver) GetServiceURI() *PulsarServiceURI {
	return r.ServiceURI
}

func (r *pulsarServiceNameResolver) GetAddressList() []*url.URL {
	return r.AddressList
}
