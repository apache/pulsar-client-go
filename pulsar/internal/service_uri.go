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
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

const (
	BinaryService = "pulsar"
	HTTPService   = "http"
	HTTPSService  = "https"
	SSLService    = "ssl"
	BinaryPort    = 6650
	BinaryTLSPort = 6651
	HTTPPort      = 80
	HTTPSPort     = 443
)

type PulsarServiceURI struct {
	ServiceName  string
	ServiceInfos []string
	ServiceHosts []string
	servicePath  string
	URL          *url.URL
}

func NewPulsarServiceURIFromURIString(uri string) (*PulsarServiceURI, error) {
	u, err := fromString(uri)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	return u, nil
}

func (p *PulsarServiceURI) UseTLS() bool {
	return p.ServiceName == HTTPSService || slices.Contains(p.ServiceInfos, SSLService)
}

func (p *PulsarServiceURI) PrimaryHostName() (string, error) {
	if len(p.ServiceHosts) > 0 {
		host, _, err := net.SplitHostPort(p.ServiceHosts[0])
		if err != nil {
			return "", err
		}
		return host, nil
	}

	return "", errors.New("no hosts available in ServiceHosts")
}

func (p *PulsarServiceURI) IsHTTP() bool {
	return strings.HasPrefix(p.ServiceName, HTTPService)
}

func fromString(uriStr string) (*PulsarServiceURI, error) {
	if uriStr == "" {
		return nil, errors.New("service URI cannot be empty")
	}

	// 1. Find first host delimiter (, or ;)
	firstDelimIdx := strings.IndexFunc(uriStr, splitURI)

	var singleHostURI string
	var additionalHosts string

	if firstDelimIdx >= 0 {
		remainder := uriStr[firstDelimIdx:]
		endIdx := strings.IndexAny(remainder, "/?#")

		if endIdx >= 0 {
			// pulsar://h1:6650,h2:6650/path
			singleHostURI = uriStr[:firstDelimIdx] + remainder[endIdx:]
			additionalHosts = remainder[1:endIdx]
		} else {
			// pulsar://h1:6650,h2:6650
			singleHostURI = uriStr[:firstDelimIdx]
			additionalHosts = remainder[1:]
		}
	} else {
		singleHostURI = uriStr
	}

	// 2. Parse single-host URI ONLY
	u, err := url.Parse(singleHostURI)
	if err != nil {
		return nil, err
	}

	if u.Host == "" {
		return nil, errors.New("service host cannot be empty")
	}

	// 3. Parse scheme
	scheme := strings.ToLower(u.Scheme)
	if scheme == "" {
		return nil, errors.New("service scheme cannot be empty")
	}

	schemeParts := strings.Split(scheme, "+")
	serviceName := schemeParts[0]
	serviceInfos := schemeParts[1:]

	// reject unknown scheme
	switch serviceName {
	case BinaryService, HTTPService, HTTPSService:
	default:
		return nil, fmt.Errorf("unsupported service name: %s", serviceName)
	}

	// 4. Validate first host
	firstHost, err := validateHostName(serviceName, serviceInfos, u.Host)
	if err != nil {
		return nil, err
	}

	serviceHosts := []string{firstHost}

	// 5. Validate remaining hosts
	if additionalHosts != "" {
		for _, h := range strings.FieldsFunc(additionalHosts, splitURI) {
			host, err := validateHostName(serviceName, serviceInfos, h)
			if err != nil {
				return nil, err
			}
			serviceHosts = append(serviceHosts, host)
		}
	}

	return &PulsarServiceURI{
		ServiceName:  serviceName,
		ServiceInfos: serviceInfos,
		ServiceHosts: serviceHosts,
		servicePath:  u.Path,
		URL:          u,
	}, nil
}

func splitURI(r rune) bool {
	return r == ',' || r == ';'
}

func validateHostName(serviceName string, serviceInfos []string, hostname string) (string, error) {
	// Trim whitespace to avoid accepting accidental invalid inputs
	hostname = strings.TrimSpace(hostname)
	if hostname == "" {
		return "", errors.New("hostname is empty")
	}

	var host, port string

	// Attempt to split host and port using the standard library.
	//
	// net.SplitHostPort enforces strict host:port syntax:
	//   - IPv4: "127.0.0.1:6650"
	//   - IPv6: "[fec0::1]:6650"
	//
	// It will fail for:
	//   - hosts without a port
	//   - bare IPv6 literals without brackets (e.g. "fec0::1")
	host, port, err := net.SplitHostPort(hostname)
	if err != nil {
		// If the hostname contains ':' but is not bracketed, it is very likely
		// an invalid IPv6 literal or an invalid host with too many colons.
		//
		// Examples rejected here:
		//   - "fec0::1"
		//   - "fec0::1:6650"
		//   - "localhost:6650:6651"
		if strings.Count(hostname, ":") > 0 && !strings.HasPrefix(hostname, "[") {
			return "", fmt.Errorf("invalid address (maybe missing brackets for IPv6 or too many colons): %s", hostname)
		}

		// Otherwise, treat the entire hostname as host without an explicit port.
		// In this case, we fall back to the default port derived from the service.
		host = hostname
		p := getServicePort(serviceName, serviceInfos)
		if p == -1 {
			return "", fmt.Errorf("no port found")
		}
		port = strconv.Itoa(p)
	}

	// Remove surrounding brackets if present.
	//
	// Note:
	//   "[fec0::1]" is NOT an IPv6 address itself, but a URI/host syntax
	//   representation used to disambiguate IPv6 literals when ports are involved.
	//   We strip brackets here so the address can be validated and normalized.
	cleanHost := host
	if len(host) >= 2 && host[0] == '[' && host[len(host)-1] == ']' {
		cleanHost = host[1 : len(host)-1]
	}

	return net.JoinHostPort(cleanHost, port), nil
}

func getServicePort(serviceName string, serviceInfos []string) int {
	switch serviceName {
	case BinaryService:
		if slices.ContainsFunc(serviceInfos, func(s string) bool {
			return strings.ToLower(s) == SSLService
		}) {
			return BinaryTLSPort
		}
		return BinaryPort
	case HTTPService:
		return HTTPPort
	case HTTPSService:
		return HTTPSPort
	}
	return -1
}
