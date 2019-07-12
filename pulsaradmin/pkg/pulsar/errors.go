package pulsar

import "fmt"

const unknownErrorReason = "Unknown pulsar error"

type Error struct {
	Reason 	string `json:"reason"`
	Code	int
}

func (e Error) Error() string {
	return fmt.Sprintf("code: %d reason: %s", e.Code, e.Reason)
}

func IsAdminError(err error) bool {
	_, ok := err.(Error)
	return ok
}
