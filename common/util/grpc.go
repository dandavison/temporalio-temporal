package util

import "google.golang.org/grpc/status"

// GetRPCStatus returns the gRPC status carried by err, or false if err is not a gRPC-induced
// error.
//
// Wrapped gRPC status errors are supported, but wrapped errors implementing
// Status() must be unwrapped before calling GetRPCStatus.
func GetRPCStatus(err error) (*status.Status, bool) {
	// This isn't correct, but is to maintain existing behavior.
	//
	// Exposing gRPC errors via `Status()` was a convention that existed for several years,
	// but the canonical way to expose error status (from google.golang.org/grpc/status) is
	// by an `GRPCStatus() *status.Status` method. [status.FromError] below does unwrapping
	// and checks for that.
	if stGetter, ok := err.(interface{ Status() *status.Status }); ok {
		return stGetter.Status(), true
	}
	return status.FromError(err)
}
