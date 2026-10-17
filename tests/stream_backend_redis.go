package tests

import (
	"context"
	"testing"
)

// redisStreamBackend stores streams in Redis, as a customer's worker or client would.
type redisStreamBackend struct{}

func newRedisStreamBackend(*testing.T) streamBackendUnderTest {
	b := &redisStreamBackend{}
	return streamBackendUnderTest{
		name:          "Redis",
		streamBackend: b,
		deleteStream:  b.deleteStream,
	}
}

func (b *redisStreamBackend) Append(context.Context, streamRef, streamAppendRequest) (streamAppendResponse, error) {
	return streamAppendResponse{}, errStreamNotImplemented
}

func (b *redisStreamBackend) Close(context.Context, streamRef, streamCloseRequest) error {
	return errStreamNotImplemented
}

func (b *redisStreamBackend) Read(context.Context, streamRef, streamReadRequest) (streamReadResponse, error) {
	return streamReadResponse{}, errStreamNotImplemented
}

func (b *redisStreamBackend) deleteStream(context.Context, streamRef) error {
	return errStreamNotImplemented
}
