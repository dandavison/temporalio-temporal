package callback

import (
	"maps"
	"time"

	callbackpb "go.temporal.io/api/callback/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// CompletionSource is the interface different kinds of executions implement so that their result can be
// delivered to waiting callback handlers.
type CompletionSource interface {
	// GetNexusCompletion returns the execution's result. Links on the returned CompleteOperationOptions
	// are the "backlinks", so a Nexus handler receiving the completion can link to its source.
	GetNexusCompletion(ctx chasm.Context, requestID string) (nexusrpc.CompleteOperationOptions, error)
}

var _ chasm.Component = (*Callback)(nil)
var _ chasm.StateMachine[callbackspb.CallbackStatus] = (*Callback)(nil)

// Callback represents a callback component in CHASM.
type Callback struct {
	chasm.UnimplementedComponent

	// Persisted internal state
	*callbackspb.CallbackState

	// Interface to retrieve Nexus operation completion data
	CompletionSource chasm.ParentPtr[CompletionSource]
}

func NewCallback(
	requestID string,
	registrationTime *timestamppb.Timestamp,
	cb *callbackspb.Callback,
) *Callback {
	return &Callback{
		CallbackState: &callbackspb.CallbackState{
			RequestId:        requestID,
			RegistrationTime: registrationTime,
			Callback:         cb,
			Status:           callbackspb.CALLBACK_STATUS_STANDBY,
		},
	}
}

func (c *Callback) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	switch c.Status {
	case callbackspb.CALLBACK_STATUS_SUCCEEDED:
		return chasm.LifecycleStateCompleted
	case callbackspb.CALLBACK_STATUS_FAILED:
		return chasm.LifecycleStateFailed
	default:
		return chasm.LifecycleStateRunning
	}
}

func (c *Callback) StateMachineState() callbackspb.CallbackStatus {
	return c.Status
}

func (c *Callback) SetStateMachineState(status callbackspb.CallbackStatus) {
	c.Status = status
}

func (c *Callback) recordAttempt(ts time.Time) {
	c.Attempt++
	c.LastAttemptCompleteTime = timestamppb.New(ts)
}

// ToAPICallback converts a CHASM callback to API callback proto.
func (c *Callback) ToAPICallback() (*commonpb.Callback, error) {
	chasmCB := c.GetCallback()
	res := &commonpb.Callback{
		// NOTE: We intentionally do not include links that were added at runtime,
		// e.g. when the callback was invoked. Those are included in ToAPICallbackInfo.
		Links: common.CloneProtoSlice(chasmCB.GetLinks()),
	}

	switch variant := chasmCB.GetVariant().(type) {
	case *callbackspb.Callback_Nexus_:
		res.Variant = &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{
				Url:    variant.Nexus.GetUrl(),
				Header: maps.Clone(variant.Nexus.GetHeader()),
			},
		}
		return res, nil
	case *callbackspb.Callback_NexusHandler_:
		res.Variant = &commonpb.Callback_NexusHandler_{
			NexusHandler: &commonpb.Callback_NexusHandler{
				TaskQueueName: variant.NexusHandler.GetTaskQueueName(),
				Service:       variant.NexusHandler.GetService(),
				Operation:     variant.NexusHandler.GetOperation(),
				SourceContext: proto.CloneOf(variant.NexusHandler.GetSourceContext()),
			},
		}
		return res, nil
	default:
		return nil, serviceerror.NewInternalf("unsupported CHASM callback type: %T", variant)
	}
}

// APIState converts the CHASM callback status to the API CallbackState enum along with the relevant
// circuit breaker's blocking status.
func (c *Callback) APIState(ctx chasm.Context) (enumspb.CallbackState, string, error) {
	state, err := c.apiStatus()
	if err != nil {
		return enumspb.CALLBACK_STATE_UNSPECIFIED, "", err
	}

	// The circuit breaker is only relevant for scheduled callbacks.
	if state != enumspb.CALLBACK_STATE_SCHEDULED {
		return state, "", nil
	}

	cbCtx := callbackContextFromChasm(ctx)
	destination, err := callbackDestination(c.GetCallback())
	if err != nil {
		return enumspb.CALLBACK_STATE_UNSPECIFIED, "", err
	}
	if !cbCtx.destinationBlocked(ctx.ExecutionKey().NamespaceID, destination) {
		return state, "", nil
	}
	return enumspb.CALLBACK_STATE_BLOCKED, "The circuit breaker is open.", nil
}

func (c *Callback) apiStatus() (enumspb.CallbackState, error) {
	switch c.Status {
	case callbackspb.CALLBACK_STATUS_STANDBY:
		return enumspb.CALLBACK_STATE_STANDBY, nil
	case callbackspb.CALLBACK_STATUS_SCHEDULED:
		return enumspb.CALLBACK_STATE_SCHEDULED, nil
	case callbackspb.CALLBACK_STATUS_BACKING_OFF:
		return enumspb.CALLBACK_STATE_BACKING_OFF, nil
	case callbackspb.CALLBACK_STATUS_FAILED:
		return enumspb.CALLBACK_STATE_FAILED, nil
	case callbackspb.CALLBACK_STATUS_SUCCEEDED:
		return enumspb.CALLBACK_STATE_SUCCEEDED, nil
	case callbackspb.CALLBACK_STATUS_UNSPECIFIED:
		return enumspb.CALLBACK_STATE_UNSPECIFIED, serviceerror.NewInternal("callback with UNSPECIFIED state")
	default:
		return enumspb.CALLBACK_STATE_UNSPECIFIED, serviceerror.NewInternalf("unknown callback state: %v", c.Status)
	}
}

// ToAPICallbackInfo returns the API CallbackInfo based on the current state of the CHASM component.
func (c *Callback) ToAPICallbackInfo(ctx chasm.Context) (*callbackpb.CallbackInfo, error) {
	apiCb, err := c.ToAPICallback()
	if err != nil {
		return nil, err
	}
	// Merge the static links that were part of the callback's creation (apiCb.Links) with
	// any new links picked up as part of the callback's execution.
	newLinks := ctx.Links(c)
	apiCb.Links = common.CloneProtoSlice(append(apiCb.Links, newLinks...))
	apiState, blockedReason, err := c.APIState(ctx)
	if err != nil {
		return nil, err
	}

	info := &callbackpb.CallbackInfo{
		Callback:                apiCb,
		RegistrationTime:        proto.CloneOf(c.RegistrationTime),
		State:                   apiState,
		BlockedReason:           blockedReason,
		RequestId:               c.RequestId,
		Attempt:                 c.Attempt,
		LastAttemptCompleteTime: proto.CloneOf(c.LastAttemptCompleteTime),
		LastAttemptFailure:      proto.CloneOf(c.LastAttemptFailure),
		NextAttemptScheduleTime: proto.CloneOf(c.NextAttemptScheduleTime),
	}
	return info, nil
}

// FromAPICallback converts an API callback into a CHASM callback proto.
func FromAPICallback(cb *commonpb.Callback) (*callbackspb.Callback, error) {
	res := &callbackspb.Callback{
		Links: common.CloneProtoSlice(cb.GetLinks()),
	}

	switch variant := cb.GetVariant().(type) {
	case *commonpb.Callback_Nexus_:
		res.Variant = &callbackspb.Callback_Nexus_{
			Nexus: &callbackspb.Callback_Nexus{
				Url:    variant.Nexus.GetUrl(),
				Header: maps.Clone(variant.Nexus.GetHeader()),
			},
		}
		return res, nil
	case *commonpb.Callback_NexusHandler_:
		res.Variant = &callbackspb.Callback_NexusHandler_{
			NexusHandler: &callbackspb.Callback_NexusHandler{
				TaskQueueName: variant.NexusHandler.GetTaskQueueName(),
				Service:       variant.NexusHandler.GetService(),
				Operation:     variant.NexusHandler.GetOperation(),
				SourceContext: proto.CloneOf(variant.NexusHandler.GetSourceContext()),
			},
		}
		return res, nil
	default:
		return nil, serviceerror.NewInvalidArgumentf("unsupported callback variant: %T", variant)
	}
}

// ScheduleStandbyCallbacks transitions all STANDBY callbacks to SCHEDULED state,
// triggering their invocation. Used by both workflows and standalone activities
// when the execution reaches a terminal state.
func ScheduleStandbyCallbacks(ctx chasm.MutableContext, callbacks chasm.Map[string, *Callback]) error {
	for _, field := range callbacks {
		cb := field.Get(ctx)
		if cb.Status != callbackspb.CALLBACK_STATUS_STANDBY {
			continue
		}
		if err := TransitionScheduled.Apply(cb, ctx, EventScheduled{}); err != nil {
			return err
		}
	}
	return nil
}
