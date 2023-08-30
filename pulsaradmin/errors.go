package pulsaradmin

import (
	"errors"
	"net/http"
)

// APIErr is an error described in the Pulsar Admin API contract with a defined reson.
type APIErr interface {
	error
	Reason() string
	Code() int
}

// ServerErr is an unexpected or abnormal error, such as an error 500 with stack
// trace that is not described by the Pulsar Admin API
type ServerErr interface {
	error
	Response() string
	Code() int
}

type codedErr interface {
	Code() int
}

// IsNotFound will return true if the error represents a Pulsar resource that
// does not exist.
func IsNotFound(err error) bool {
	var ce codedErr
	if errors.As(err, &ce) {
		return ce.Code() == http.StatusNotFound
	}
	return false
}
