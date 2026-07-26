package tests

// The standalone-activity test environment, and the payload its activities are started with. These sit
// outside a _test.go file because the drivers, which are not test files, refer to them.

import (
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
)

type standaloneActivityEnv struct {
	*testcore.TestEnv
}

var defaultInput = payloads.EncodeString("Input")
