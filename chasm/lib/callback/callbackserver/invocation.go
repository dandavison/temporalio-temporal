package callbackserver

import (
	"errors"
	"fmt"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log/tag"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/softassert"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
)

//nolint:revive // context.Context is an input parameter for chasm.ReadComponent, not a function parameter
func loadInvocationArgs(
	c *callback.Callback,
	ctx chasm.Context,
	_ chasm.NoValue,
) (invocable, error) {
	// Reject unknown/unsupported callback variants.
	switch c.GetCallback().GetVariant().(type) {
	case *callbackspb.Callback_Nexus_, *callbackspb.Callback_NexusHandler_:
		// OK
	default:
		return nil, queueserrors.NewUnprocessableTaskError(
			fmt.Sprintf("unprocessable callback variant: %T", c.GetCallback().GetVariant()),
		)
	}

	// Get the parent CHASM object's Nexus result to be delivered.
	target := c.CompletionSource.Get(ctx)
	completion, err := target.GetNexusCompletion(ctx, c.RequestId)
	if err != nil {
		return nil, err
	}

	// NexusHandler callbacks, deliver the result by invoking a Nexus handler.
	if nexusHandler := c.GetCallback().GetNexusHandler(); nexusHandler != nil {
		parentComponent, ok := target.(chasm.Component)
		if !ok {
			return nil, errors.New("target CompletionSource is not a CHASM component")
		}

		// Create a link pointing to *this* CHASM Callback. NexusHandler-variant callbacks do not invoke the
		// targeted Nexus handler using the links carried on the CompletionSource, because that would point to the
		// source execution. Instead, we use the Callback-variant Link to identify a particular completion callback
		// *attached to* the source execution. (e.g. Workflow Callback[2] is what spawned a given resource, and not
		// the Workflow itself.)
		var sourceCbLinks []*nexuspb.Link
		selfLink, err := buildCallbackLink(ctx, parentComponent, c.GetRequestId())
		if err != nil {
			softassert.Fail(
				ctx.Logger(),
				"failed to build the callback self link",
				tag.Error(err),
				tag.NexusCompletionSource(c.CompletionSource.Fqn()),
			)
		} else {
			sourceCbLinks = commonnexus.ConvertLinksToProto([]nexus.Link{selfLink})
		}

		return invocableNexusHandler{
			callback:            nexusHandler,
			completion:          completion,
			sourceLinks:         sourceCbLinks,
			completionSourceTag: c.CompletionSource.Fqn(),
			businessID:          ctx.ExecutionKey().BusinessID,
			runID:               ctx.ExecutionKey().RunID,
			requestID:           c.RequestId,
			attempt:             c.Attempt,
		}, nil
	}

	// Nexus callbacks deliver results by also invoking a Nexus handler, but using HTTP.
	nexusCallback := c.GetCallback().GetNexus()
	if nexusCallback.GetUrl() == chasm.NexusCompletionHandlerURL {
		return invocableInternal{
			callback:   nexusCallback,
			attempt:    c.Attempt,
			completion: completion,
			requestID:  c.RequestId,
		}, nil
	}
	return invocableOutbound{
		callback:            nexusCallback,
		completion:          completion,
		completionSourceTag: c.CompletionSource.Fqn(),
		businessID:          ctx.ExecutionKey().BusinessID,
		runID:               ctx.ExecutionKey().RunID,
		attempt:             c.Attempt,
	}, nil
}

// recordHandlerLinks stores the links the callback's target returned when it accepted the delivery.
//
// For a NexusHandler callback these are the handler links the worker attached to its StartOperation
// response, e.g. a workflow_event link to the workflow it started to process the completion.
func recordHandlerLinks(c *callback.Callback, ctx chasm.MutableContext, links []nexus.Link) error {
	if len(links) == 0 {
		return nil
	}
	// Unconvertible links are dropped with a warning rather than failing the callback. The callback
	// has already been delivered at this point, so returning an error would fail the transition and
	// leave the delivered callback retrying forever.
	protoLinks := commonnexus.ConvertNexusLinksToProtoLinks(links, ctx.Logger())
	if len(protoLinks) == 0 {
		return nil
	}
	// Swallow any errors here for the same reason as above.
	if err := ctx.SetRequestLinks(c, c.RequestId, protoLinks); err != nil {
		softassert.Fail(ctx.Logger(), "failed to record NexusHandler callback links", tag.Error(err))
	}
	return nil
}

type saveResultInput struct {
	result      invocationResult
	retryPolicy backoff.RetryPolicy
}

func saveResult(
	c *callback.Callback,
	ctx chasm.MutableContext,
	input saveResultInput,
) (chasm.NoValue, error) {
	switch r := input.result.(type) {
	case invocationResultOK:
		// Persist any links returned from the callback's invocation.
		if err := recordHandlerLinks(c, ctx, r.links); err != nil {
			return nil, err
		}
		err := callback.TransitionSucceeded.Apply(c, ctx, callback.EventSucceeded{Time: ctx.Now(c)})
		return nil, err
	case invocationResultRetry:
		err := callback.TransitionAttemptFailed.Apply(c, ctx, callback.EventAttemptFailed{
			Time:        ctx.Now(c),
			Err:         r.err,
			RetryPolicy: input.retryPolicy,
		})
		return nil, err
	case invocationResultFail:
		err := callback.TransitionFailed.Apply(c, ctx, callback.EventFailed{
			Time: ctx.Now(c),
			Err:  r.err,
		})
		return nil, err
	default:
		return nil, queueserrors.NewUnprocessableTaskError(
			fmt.Sprintf("unrecognized callback result %v", input.result),
		)
	}
}

// buildCallbackLink returns a commonpb.Link_Callback encoded as a nexus.Link, referring to
// the given CHASM component.
func buildCallbackLink(ctx chasm.Context, parentComponent chasm.Component, cbRequestID string) (nexus.Link, error) {
	chasmExInfo := ctx.ExecutionInfo()
	chasmExKey := ctx.ExecutionKey()

	execution := &commonpb.Execution{
		Type:       chasmExInfo.ExecutionType,
		BusinessId: chasmExKey.BusinessID,
		RunId:      chasmExKey.RunID,
	}

	link, err := commonnexus.ConvertLinkCallbackToNexusLink(&commonpb.Link_Callback{
		Namespace:     ctx.NamespaceEntry().Name().String(),
		Execution:     execution,
		ComponentPath: ctx.Path(parentComponent),
		RequestId:     cbRequestID,
	})
	if err != nil {
		return nexus.Link{}, fmt.Errorf("converting to nexus.Link: %w", err)
	}
	return link, nil
}
