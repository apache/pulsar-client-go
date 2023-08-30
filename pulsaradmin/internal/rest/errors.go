package rest

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

type apiError struct {
	reason string
	code   int
}

func (e *apiError) Error() string {
	return e.reason
}

func (e *apiError) Reason() string {
	return e.reason
}

func (e *apiError) Code() int {
	return e.code
}

// serverError is an unexpected or fatal error from the server.
type serverError struct {
	// response is the response body returned by the server.
	response string
	// code is the HTTP code returned from the server
	code int
}

func (e *serverError) Error() string {
	if len(e.response) > 0 {
		return fmt.Sprintf("%d-%s\n%s", e.code, http.StatusText(e.code), e.response)
	}
	return fmt.Sprintf("%d-%s", e.code, http.StatusText(e.code))
}

func (e *serverError) Response() string {
	return e.response
}

func (e *serverError) Code() int {
	return e.code
}

// IsAdminErr will return true if the server returns an administrative error with
// a `reason`
func IsAdminErr(err error) bool {
	_, ok := err.(*apiError)
	return ok
}

// responseError will convert an HTTP response to either an *AdminError or a
// *RequestError, depending on whether ar `reason` was found.
func responseError(resp *http.Response) error {
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return &serverError{
			response: fmt.Sprintf("%d-%s - response body could not be read: %v", resp.StatusCode, resp.Status, err),
			code:     resp.StatusCode,
		}
	}
	jsonResp := struct {
		Reason string `json:"reason"`
	}{}
	if len(body) > 0 && body[0] == '{' {
		// the particulars of the unmarshal error are not important. If it is
		// not well-formatted JSON, then it's not an apiError and, by
		// definition, unexpected.
		_ = json.Unmarshal(body, &jsonResp)
	}
	if jsonResp.Reason != "" {
		return &apiError{
			reason: jsonResp.Reason,
			code:   resp.StatusCode,
		}
	}

	return &serverError{
		code:     resp.StatusCode,
		response: string(body),
	}
}
