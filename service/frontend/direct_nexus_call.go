package frontend

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
)

// StartNexusOperation calls an operation of the System Nexus Endpoint directly, without a
// NexusOperation component. Prototype: only the System Nexus Endpoint and synchronous operations are
// supported, and links are not.
func (wh *WorkflowHandler) StartNexusOperation(
	ctx context.Context,
	request *workflowservice.StartNexusOperationRequest,
) (_ *workflowservice.StartNexusOperationResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	ns, err := wh.directNexusCallNamespace(request.GetNamespace(), request.GetEndpoint())
	if err != nil {
		return nil, err
	}
	if len(request.GetLinks()) > 0 {
		return nil, serviceerror.NewInvalidArgument("links are not supported on direct Nexus calls")
	}
	requestID := request.GetRequestId()
	if requestID == "" {
		requestID = uuid.NewString()
	}
	res, err := wh.registry.NexusEndpointProcessor.ProcessInput(chasm.NexusOperationProcessorContext{
		Namespace:               ns,
		RequestID:               requestID,
		ReserializeInputPayload: true,
	}, request.GetService(), request.GetOperation(), request.GetInput())
	if err != nil {
		return nil, handlerErrorToServiceError(err)
	}
	resp, err := wh.historyClient.StartNexusOperation(ctx, &historyservice.StartNexusOperationRequest{
		NamespaceId: ns.ID().String(),
		ShardId:     res.RoutingKey.ShardID(wh.config.NumHistoryShards),
		Request: &nexuspb.StartOperationRequest{
			Service:   request.GetService(),
			Operation: request.GetOperation(),
			Payload:   res.ReserializedInputPayload,
			RequestId: requestID,
		},
	})
	if err != nil {
		return nil, err
	}
	switch v := resp.GetResponse().GetVariant().(type) {
	case *nexuspb.StartOperationResponse_SyncSuccess:
		return &workflowservice.StartNexusOperationResponse{
			Variant: &workflowservice.StartNexusOperationResponse_SyncSuccess_{
				SyncSuccess: &workflowservice.StartNexusOperationResponse_SyncSuccess{Payload: v.SyncSuccess.GetPayload()},
			},
			Links: v.SyncSuccess.GetLinks(),
		}, nil
	case *nexuspb.StartOperationResponse_Failure:
		return &workflowservice.StartNexusOperationResponse{
			Variant: &workflowservice.StartNexusOperationResponse_OperationFailure_{
				OperationFailure: &workflowservice.StartNexusOperationResponse_OperationFailure{Failure: v.Failure},
			},
		}, nil
	default:
		return nil, serviceerror.NewUnimplementedf("direct Nexus calls support only synchronous operations, got %T", v)
	}
}

// CancelNexusOperation is not supported: direct Nexus calls are synchronous only.
func (wh *WorkflowHandler) CancelNexusOperation(
	context.Context,
	*workflowservice.CancelNexusOperationRequest,
) (*workflowservice.CancelNexusOperationResponse, error) {
	return nil, serviceerror.NewUnimplemented("direct Nexus calls support only synchronous operations")
}

// handlerErrorToServiceError converts a Nexus handler error from input processing to a gRPC error.
func handlerErrorToServiceError(err error) error {
	he, ok := errors.AsType[*nexus.HandlerError](err)
	if !ok {
		return err
	}
	switch he.Type {
	case nexus.HandlerErrorTypeBadRequest:
		return serviceerror.NewInvalidArgument(he.Error())
	case nexus.HandlerErrorTypeNotFound:
		return serviceerror.NewNotFound(he.Error())
	case nexus.HandlerErrorTypeUnauthorized, nexus.HandlerErrorTypeUnauthenticated:
		return serviceerror.NewPermissionDenied(he.Error(), "")
	case nexus.HandlerErrorTypeNotImplemented:
		return serviceerror.NewUnimplemented(he.Error())
	default:
		return serviceerror.NewInternal(he.Error())
	}
}

func (wh *WorkflowHandler) directNexusCallNamespace(name, endpoint string) (*namespace.Namespace, error) {
	if name == "" {
		return nil, errNamespaceNotSet
	}
	if !wh.config.EnableDirectNexusCalls(name) {
		return nil, serviceerror.NewUnimplemented("direct Nexus calls are disabled")
	}
	if endpoint != commonnexus.SystemEndpoint {
		return nil, serviceerror.NewUnimplementedf("direct Nexus calls support only the %q endpoint", commonnexus.SystemEndpoint)
	}
	return wh.namespaceRegistry.GetNamespace(namespace.Name(name))
}
