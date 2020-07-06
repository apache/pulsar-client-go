// Copyright (c) 2020 StreamNative, Inc.. All Rights Reserved.

package store

import (
	"errors"

	"github.com/apache/pulsar-client-go/oauth2"
)

// ErrNoAuthenticationData indicates that stored authentication data is not available
var ErrNoAuthenticationData = errors.New("authentication data is not available")

// ErrUnsupportedAuthData ndicates that stored authentication data is unusable
var ErrUnsupportedAuthData = errors.New("authentication data is not usable")

// Store is responsible for persisting authorization grants
type Store interface {
	// SaveGrant stores an authorization grant for a given audience
	SaveGrant(audience string, grant auth.AuthorizationGrant) error

	// LoadGrant loads an authorization grant for a given audience
	LoadGrant(audience string) (*auth.AuthorizationGrant, error)

	// WhoAmI returns the current user name (or an error if nobody is logged in)
	WhoAmI(audience string) (string, error)

	// Logout deletes all stored credentials
	Logout() error
}
