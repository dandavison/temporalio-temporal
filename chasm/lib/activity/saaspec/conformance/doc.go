// Package conformance holds static checks comparing the saaspec spec against the activity
// state-machine code, without a server.
//
//   - TestModelDecisionCoverage: Model() is total over the RPC domain — every (status, event) cell
//     returns an Outcome or is an explicit unreachable assertion; any other panic fails.
//
//   - TestModelEdgesReachableInCode: every status change Model() accepts (A -> B) is reachable from A
//     by the code's declared transitions (chained transitions allowed).
package conformance
