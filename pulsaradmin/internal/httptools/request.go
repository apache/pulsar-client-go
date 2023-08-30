package httptools

import (
	"context"
	"net/http"
)

func CloneReq(r *http.Request) *http.Request {
	ctx := r.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	return r.Clone(ctx)
}
