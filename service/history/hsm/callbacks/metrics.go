package callbacks

import (
	"go.temporal.io/server/chasm/lib/callback/callbackserver"
)

var (
	RequestCounter          = callbackserver.RequestCounter
	RequestLatencyHistogram = callbackserver.RequestLatencyHistogram
)
