package stream

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
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
	request := req.GetFrontendRequest()
	key := chasm.ExecutionKey{
		NamespaceID: request.GetNamespace(),
		BusinessID:  request.GetStreamId(),
		RunID:       request.GetRunId(),
	}

	// This is an upsert: we have not added a separate CreateStream API. However, CHASM lacks an
	// upsert API, so we are doing two transactions currently.
	// TODO: do this in one transaction

	createFn := func() (int64, chasm.ExecutionKey, []byte, error) {
		return chasm.NewExecution(
			ctx,
			key,
			func(ctx chasm.MutableContext, req *workflowservice.PushStreamRequest) (*Stream, int64, error) {
				return newStream(req), 0, nil
			},
			request,
		)
	}
	updateFn := func() (int64, []byte, error) {
		return chasm.UpdateComponent(
			ctx,
			chasm.NewComponentRef[*Stream](key),
			(*Stream).AddMessages,
			request.GetMessages(),
		)
	}

	// TODO: ignored return values
	_, _, err := updateFn()
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		_, _, _, err := createFn()
		if err != nil {
			return nil, err
		}
		_, _, err = updateFn()
	}
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
