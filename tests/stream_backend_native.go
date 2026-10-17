package tests

import (
	"context"

	"go.temporal.io/server/tests/testcore"
)

// nativeStreamBackend is a client of the frontend's stream RPCs.
type nativeStreamBackend struct {
	env *testcore.TestEnv
}

func newNativeStreamBackend(env *testcore.TestEnv) streamBackendUnderTest {
	b := &nativeStreamBackend{env: env}
	return streamBackendUnderTest{
		name:          "Native",
		streamBackend: b,
		native:        true,
		deleteStream:  b.deleteStream,
		offset:        b.offset,
	}
}

func (b *nativeStreamBackend) Append(context.Context, streamRef, streamAppendRequest) (streamAppendResponse, error) {
	return streamAppendResponse{}, errStreamNotImplemented
}

func (b *nativeStreamBackend) Close(context.Context, streamRef, streamCloseRequest) error {
	return errStreamNotImplemented
}

func (b *nativeStreamBackend) Read(context.Context, streamRef, streamReadRequest) (streamReadResponse, error) {
	return streamReadResponse{}, errStreamNotImplemented
}

func (b *nativeStreamBackend) deleteStream(context.Context, streamRef) error {
	return errStreamNotImplemented
}

func (b *nativeStreamBackend) offset(streamCursor) (int64, error) {
	return 0, errStreamNotImplemented
}
