// Copyright (c) 2020 StreamNative, Inc.. All Rights Reserved.

package auth

import (
	"encoding/json"
	"net/http"
	"net/url"
	"path"

	"github.com/pkg/errors"
)

// OIDCWellKnownEndpoints holds the well known OIDC endpoints
type OIDCWellKnownEndpoints struct {
	AuthorizationEndpoint       string `json:"authorization_endpoint"`
	TokenEndpoint               string `json:"token_endpoint"`
	DeviceAuthorizationEndpoint string `json:"device_authorization_endpoint"`
}

// GetOIDCWellKnownEndpointsFromIssuerURL gets the well known endpoints for the
// passed in issuer url
func GetOIDCWellKnownEndpointsFromIssuerURL(issuerURL string) (*OIDCWellKnownEndpoints, error) {
	u, err := url.Parse(issuerURL)
	if err != nil {
		return nil, errors.Wrap(err, "could not parse issuer url to build well known endpoints")
	}
	u.Path = path.Join(u.Path, ".well-known/openid-configuration")

	r, err := http.Get(u.String())
	if err != nil {
		return nil, errors.Wrapf(err, "could not get well known endpoints from url %s", u.String())
	}
	defer r.Body.Close()

	var wkEndpoints OIDCWellKnownEndpoints
	err = json.NewDecoder(r.Body).Decode(&wkEndpoints)
	if err != nil {
		return nil, errors.Wrap(err, "could not decode json body when getting well known endpoints")
	}

	return &wkEndpoints, nil
}
