// Package conformance holds static checks that compare the saaspec behavior spec against
// the activity state-machine code, without running a server.
//
// It reads the code's declared transitions (the exported activity.Transition* values, each
// of which exposes its Sources and Destination statuses) and compares them against what
// saaspec.Model() accepts. Two checks live here:
//
//   - TestModelDecisionCoverage asserts Model() is total over the RPC domain: every (status,
//     event) cell either returns an Outcome or is an explicit unreachable assertion. Any other
//     panic fails the test.
//
//   - TestModelEdgesReachableInCode asserts that every status change the spec accepts can
//     actually be produced by the code. For each cell where Model() moves the activity from
//     status A to a different status B, B must be reachable from A by following one or more
//     declared transitions. This tolerates handlers that chain transitions (for example,
//     cancelling a scheduled activity moves it to CANCEL_REQUESTED and then to CANCELED within
//     one call).
package conformance
