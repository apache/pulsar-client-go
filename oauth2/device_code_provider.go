// Copyright (c) 2020 StreamNative, Inc.. All Rights Reserved.

package auth

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// DeviceCodeProvider holds the information needed to easily get a
// device code locally.
type LocalDeviceCodeProvider struct {
	Issuer
	oidcWellKnownEndpoints OIDCWellKnownEndpoints
	transport              HTTPAuthTransport
}

type DeviceCodeRequest struct {
	ClientID string
	Scopes   []string
	Audience string
}

// DeviceCodeResult holds the device code gotten from the device code URL.
type DeviceCodeResult struct {
	DeviceCode              string `json:"device_code"`
	UserCode                string `json:"user_code"`
	VerificationURI         string `json:"verification_uri"`
	VerificationURIComplete string `json:"verification_uri_complete"`
	ExpiresIn               int    `json:"expires_in"`
	Interval                int    `json:"interval"`
}

// NewLocalDeviceCodeProvider allows for the easy setup of LocalDeviceCodeProvider
func NewLocalDeviceCodeProvider(
	issuer Issuer,
	oidcWellKnownEndpoints OIDCWellKnownEndpoints,
	authTransport HTTPAuthTransport) *LocalDeviceCodeProvider {
	return &LocalDeviceCodeProvider{
		issuer,
		oidcWellKnownEndpoints,
		authTransport,
	}
}

// GetCode obtains a new device code. Additional scopes
// beyond openid and email can be sent by passing in arguments for
// <additionalScopes>.
func (cp *LocalDeviceCodeProvider) GetCode(additionalScopes ...string) (*DeviceCodeResult, error) {
	request, err := cp.newDeviceCodeRequest(&DeviceCodeRequest{
		ClientID: cp.ClientID,
		Scopes:   append([]string{"openid", "email"}, additionalScopes...),
		Audience: cp.Audience,
	})
	if err != nil {
		return nil, err
	}

	response, err := cp.transport.Do(request)
	if err != nil {
		return nil, err
	}

	dcr, err := cp.handleDeviceCodeResponse(response)
	if err != nil {
		return nil, err
	}

	return dcr, nil
}

// newDeviceCodeRequest builds a new DeviceCodeRequest wrapped in an
// http.Request
func (cp *LocalDeviceCodeProvider) newDeviceCodeRequest(
	req *DeviceCodeRequest) (*http.Request, error) {
	uv := url.Values{}
	uv.Set("client_id", req.ClientID)
	uv.Set("scope", strings.Join(req.Scopes, " "))
	uv.Set("audience", req.Audience)
	euv := uv.Encode()

	request, err := http.NewRequest("POST",
		cp.oidcWellKnownEndpoints.DeviceAuthorizationEndpoint,
		strings.NewReader(euv),
	)
	if err != nil {
		return nil, err
	}

	request.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Add("Content-Length", strconv.Itoa(len(euv)))

	return request, nil
}

func (cp *LocalDeviceCodeProvider) handleDeviceCodeResponse(resp *http.Response) (*DeviceCodeResult, error) {
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("a non-success status code was received: %d", resp.StatusCode)
	}

	defer resp.Body.Close()

	dcr := DeviceCodeResult{}
	err := json.NewDecoder(resp.Body).Decode(&dcr)
	if err != nil {
		return nil, err
	}

	return &dcr, nil
}
