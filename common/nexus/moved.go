package nexus

import "go.temporal.io/server/common/nexus/nexusconv"

// AdaptAuthorizeError remains here so that code outside this repository that uses it keeps building.
//
// Deprecated: use nexusconv.AdaptAuthorizeError.
var AdaptAuthorizeError = nexusconv.AdaptAuthorizeError

// CoerceToCanceledFailure remains here so that code outside this repository that uses it keeps building.
//
// Deprecated: use nexusconv.CoerceToCanceledFailure.
var CoerceToCanceledFailure = nexusconv.CoerceToCanceledFailure

// ConvertGRPCError remains here so that code outside this repository that uses it keeps building.
//
// Deprecated: use nexusconv.ConvertGRPCError.
var ConvertGRPCError = nexusconv.ConvertGRPCError

// ConvertLinkActivityToNexusLink remains here so that code outside this repository that uses it keeps building.
//
// Deprecated: use nexusconv.ConvertLinkActivityToNexusLink.
var ConvertLinkActivityToNexusLink = nexusconv.ConvertLinkActivityToNexusLink

// ConvertLinkCallbackToNexusLink remains here so that code outside this repository that uses it keeps building.
//
// Deprecated: use nexusconv.ConvertLinkCallbackToNexusLink.
var ConvertLinkCallbackToNexusLink = nexusconv.ConvertLinkCallbackToNexusLink

// ConvertLinkNexusOperationToNexusLink remains here so that code outside this repository that uses it keeps building.
//
// Deprecated: use nexusconv.ConvertLinkNexusOperationToNexusLink.
var ConvertLinkNexusOperationToNexusLink = nexusconv.ConvertLinkNexusOperationToNexusLink

// ConvertLinkWorkflowEventToNexusLink remains here so that code outside this repository that uses it keeps building.
//
// Deprecated: use nexusconv.ConvertLinkWorkflowEventToNexusLink.
var ConvertLinkWorkflowEventToNexusLink = nexusconv.ConvertLinkWorkflowEventToNexusLink

// ConvertNexusLinkToLinkActivity remains here so that code outside this repository that uses it keeps building.
//
// Deprecated: use nexusconv.ConvertNexusLinkToLinkActivity.
var ConvertNexusLinkToLinkActivity = nexusconv.ConvertNexusLinkToLinkActivity

// ConvertNexusLinkToLinkCallback remains here so that code outside this repository that uses it keeps building.
//
// Deprecated: use nexusconv.ConvertNexusLinkToLinkCallback.
var ConvertNexusLinkToLinkCallback = nexusconv.ConvertNexusLinkToLinkCallback

// ConvertNexusLinkToLinkWorkflowEvent remains here so that code outside this repository that uses it keeps building.
//
// Deprecated: use nexusconv.ConvertNexusLinkToLinkWorkflowEvent.
var ConvertNexusLinkToLinkWorkflowEvent = nexusconv.ConvertNexusLinkToLinkWorkflowEvent

// FailureSourceContextKey remains here so that code outside this repository that uses it keeps building.
//
// Deprecated: use nexusconv.FailureSourceContextKey.
var FailureSourceContextKey = nexusconv.FailureSourceContextKey

// FailureSourceHeaderName remains here so that code outside this repository that uses it keeps building.
//
// Deprecated: use nexusconv.FailureSourceHeaderName.
const FailureSourceHeaderName = nexusconv.FailureSourceHeaderName

// FailureSourceWorker remains here so that code outside this repository that uses it keeps building.
//
// Deprecated: use nexusconv.FailureSourceWorker.
const FailureSourceWorker = nexusconv.FailureSourceWorker

// NexusFailureToProtoFailure remains here so that code outside this repository that uses it keeps building.
//
// Deprecated: use nexusconv.NexusFailureToProtoFailure.
var NexusFailureToProtoFailure = nexusconv.NexusFailureToProtoFailure

// NexusFailureToTemporalFailure remains here so that code outside this repository that uses it keeps building.
//
// Deprecated: use nexusconv.NexusFailureToTemporalFailure.
var NexusFailureToTemporalFailure = nexusconv.NexusFailureToTemporalFailure

// OperationErrorToTemporalFailure remains here so that code outside this repository that uses it keeps building.
//
// Deprecated: use nexusconv.OperationErrorToTemporalFailure.
var OperationErrorToTemporalFailure = nexusconv.OperationErrorToTemporalFailure

// ProtoFailureToNexusFailure remains here so that code outside this repository that uses it keeps building.
//
// Deprecated: use nexusconv.ProtoFailureToNexusFailure.
var ProtoFailureToNexusFailure = nexusconv.ProtoFailureToNexusFailure

// SetFailureSourceOnContext remains here so that code outside this repository that uses it keeps building.
//
// Deprecated: use nexusconv.SetFailureSourceOnContext.
var SetFailureSourceOnContext = nexusconv.SetFailureSourceOnContext

// TemporalFailureToNexusFailure remains here so that code outside this repository that uses it keeps building.
//
// Deprecated: use nexusconv.TemporalFailureToNexusFailure.
var TemporalFailureToNexusFailure = nexusconv.TemporalFailureToNexusFailure

// TemporalFailureToNexusFailureInPlace remains here so that code outside this repository that uses it keeps building.
//
// Deprecated: use nexusconv.TemporalFailureToNexusFailureInPlace.
var TemporalFailureToNexusFailureInPlace = nexusconv.TemporalFailureToNexusFailureInPlace
