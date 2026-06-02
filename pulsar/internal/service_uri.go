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
	"net/netip"
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

type UnsupportedServiceNameError struct {
	ServiceName string
}

func (e *UnsupportedServiceNameError) Error() string {
	return fmt.Sprintf("unsupported service name: %s", e.ServiceName)
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
	return p.ServiceName == HTTPService || p.ServiceName == HTTPSService
}

func fromString(uriStr string) (*PulsarServiceURI, error) {
	if uriStr == "" {
		return nil, errors.New("service URI cannot be empty")
	}

	// 1. Reduce a multi-host URI to one parseable host while preserving suffixes.
	singleHostURI, additionalHosts := splitHostURI(uriStr)

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
		return nil, &UnsupportedServiceNameError{ServiceName: serviceName}
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

func splitHostURI(uriStr string) (string, string) {
	authorityStart := strings.Index(uriStr, "//")
	if authorityStart < 0 {
		return uriStr, ""
	}
	authorityStart += 2

	authorityEnd := len(uriStr)
	if authoritySuffixEnd := strings.IndexAny(uriStr[authorityStart:], "/?#"); authoritySuffixEnd >= 0 {
		authorityEnd = authorityStart + authoritySuffixEnd
	}

	hostListStart := authorityStart
	if userInfoEnd := strings.LastIndex(uriStr[authorityStart:authorityEnd], "@"); userInfoEnd >= 0 {
		hostListStart += userInfoEnd + 1
	}

	if firstDelim := strings.IndexFunc(uriStr[hostListStart:authorityEnd], splitURI); firstDelim >= 0 {
		firstDelimIdx := hostListStart + firstDelim
		return uriStr[:firstDelimIdx] + uriStr[authorityEnd:], uriStr[firstDelimIdx+1 : authorityEnd]
	}

	return uriStr, ""
}

func validateHostName(serviceName string, serviceInfos []string, hostname string) (string, error) {
	host, port, err := splitHostPortOrDefault(serviceName, serviceInfos, hostname)
	if err != nil {
		return "", err
	}

	cleanHost, err := normalizeValidatedHost(hostname, host)
	if err != nil {
		return "", err
	}

	return net.JoinHostPort(cleanHost, port), nil
}

func splitHostPortOrDefault(serviceName string, serviceInfos []string, hostname string) (string, string, error) {
	// net.SplitHostPort enforces strict host:port syntax:
	//   - IPv4: "127.0.0.1:6650"
	//   - IPv6: "[fec0::1]:6650"
	//
	// It will fail for:
	//   - hosts without a port
	//   - bare IPv6 literals without brackets (e.g. "fec0::1")
	host, port, err := net.SplitHostPort(hostname)
	if err == nil {
		if host == "" {
			// net.SplitHostPort accepts ":port" with an empty host, but we explicitly
			// reject such inputs because a non-empty hostname is required.
			return "", "", fmt.Errorf("invalid address: host is empty in %q", hostname)
		}
		return host, port, nil
	}

	// If the hostname contains ':' but is not bracketed, it is very likely
	// an invalid IPv6 literal or an invalid host with too many colons.
	//
	// Examples rejected here:
	//   - "fec0::1"
	//   - "fec0::1:6650"
	//   - "localhost:6650:6651"
	if strings.Contains(hostname, ":") && !strings.HasPrefix(hostname, "[") {
		return "", "", fmt.Errorf("invalid address (maybe missing brackets for IPv6 or too many colons): %s", hostname)
	}

	defaultPort := getServicePort(serviceName, serviceInfos)
	if defaultPort == -1 {
		return "", "", fmt.Errorf("no port found")
	}

	return hostname, strconv.Itoa(defaultPort), nil
}

func normalizeValidatedHost(hostname, host string) (string, error) {
	hasOpeningBracket := strings.HasPrefix(hostname, "[")
	hasClosingBracket := strings.Contains(hostname, "]")
	if hasOpeningBracket != hasClosingBracket {
		return "", fmt.Errorf("invalid bracketed host: %s", hostname)
	}

	cleanHost := strings.TrimPrefix(strings.TrimSuffix(host, "]"), "[")
	if !hasOpeningBracket {
		return cleanHost, nil
	}

	addr, err := netip.ParseAddr(cleanHost)
	if err != nil || !addr.Is6() {
		return "", fmt.Errorf("invalid IPv6 address: %s", hostname)
	}

	return cleanHost, nil
}

func getServicePort(serviceName string, serviceInfos []string) int {
	switch serviceName {
	case BinaryService:
		// For Pulsar, only the "ssl" modifier is allowed. Any other non-empty
		// modifier is treated as invalid and causes port resolution to fail.
		if len(serviceInfos) == 0 {
			return BinaryPort
		}

		hasSSL := false
		for _, info := range serviceInfos {
			if info == "" {
				// Ignore empty modifiers if present.
				continue
			}
			if strings.EqualFold(info, SSLService) {
				hasSSL = true
				continue
			}
			// Unknown modifier: reject to avoid silently accepting typos.
			return -1
		}

		if hasSSL {
			return BinaryTLSPort
		}
		return BinaryPort
	case HTTPService:
		// HTTP should not have any scheme modifiers; reject if present.
		if len(serviceInfos) != 0 {
			return -1
		}
		return HTTPPort
	case HTTPSService:
		// HTTPS should not have any scheme modifiers; reject if present.
		if len(serviceInfos) != 0 {
			return -1
		}
		return HTTPSPort
	}
	return -1
}
