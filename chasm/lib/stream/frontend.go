package stream

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/namespace"
)

// StreamFrontendHandler handles stream-related frontend requests.
type StreamFrontendHandler interface {
	PushStream(ctx context.Context, req *workflowservice.PushStreamRequest) (*workflowservice.PushStreamResponse, error)
	PollStream(ctx context.Context, req *workflowservice.PollStreamRequest) (*workflowservice.PollStreamResponse, error)
}

type frontendHandler struct {
	client            streampb.StreamServiceClient
	namespaceRegistry namespace.Registry
}

// NewFrontendHandler creates a new FrontendHandler instance.
func NewFrontendHandler(
	client streampb.StreamServiceClient,
	namespaceRegistry namespace.Registry,
) StreamFrontendHandler {
	return &frontendHandler{
		client:            client,
		namespaceRegistry: namespaceRegistry,
	}
}

// PushStream pushes messages to the stream.
func (h *frontendHandler) PushStream(
	ctx context.Context,
	req *workflowservice.PushStreamRequest,
) (*workflowservice.PushStreamResponse, error) {
	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}

	resp, err := h.client.PushStream(ctx, &streampb.PushStreamRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})
	if err != nil {
		return nil, err
	}

	return resp.GetFrontendResponse(), nil
}

// PollStream long-polls for new messages on the stream.
func (h *frontendHandler) PollStream(
	ctx context.Context,
	req *workflowservice.PollStreamRequest,
) (*workflowservice.PollStreamResponse, error) {
	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}

	resp, err := h.client.PollStream(ctx, &streampb.PollStreamRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})
	if err != nil {
		return nil, err
	}

	return resp.GetFrontendResponse(), nil
}
