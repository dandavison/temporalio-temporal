package callbackserver

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
)

func TestLoadInvocationArgsUnsupportedVariant(t *testing.T) {
	cb := &callback.Callback{
		CallbackState: &callbackspb.CallbackState{
			Callback: &callbackspb.Callback{},
		},
	}
	_, err := loadInvocationArgs(cb, &chasm.MockMutableContext{}, nil)

	var unprocessableErr *queueserrors.UnprocessableTaskError
	require.ErrorAs(t, err, &unprocessableErr)
	require.ErrorContains(t, err, "unprocessable callback variant")
}
