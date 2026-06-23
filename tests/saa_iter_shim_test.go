package tests

import (
	"context"

	"go.temporal.io/server/common/nexus/nexusrpc"
)

type completionHandler struct {
	requestCh         chan *nexusrpc.CompletionRequest
	requestCompleteCh chan error
}

func (h *completionHandler) CompleteOperation(ctx context.Context, request *nexusrpc.CompletionRequest) error {
	h.requestCh <- request
	return <-h.requestCompleteCh
}
