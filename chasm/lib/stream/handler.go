package stream

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

type handler struct {
	streampb.UnimplementedStreamServiceServer
}

func newHandler() *handler {
	return &handler{}
}

// PushStream pushes messages to the stream.
func (h *handler) PushStream(
	ctx context.Context,
	req *streampb.PushStreamRequest,
) (*streampb.PushStreamResponse, error) {
	return &streampb.PushStreamResponse{
		FrontendResponse: &workflowservice.PushStreamResponse{},
	}, nil
}

// PollStream long-polls for new messages on the stream.
func (h *handler) PollStream(
	ctx context.Context,
	req *streampb.PollStreamRequest,
) (*streampb.PollStreamResponse, error) {
	return &streampb.PollStreamResponse{
		FrontendResponse: &workflowservice.PollStreamResponse{},
	}, nil
}
