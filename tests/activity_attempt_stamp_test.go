package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

// An activity task's attempt_stamp identifies the attempt among all attempts of the activity: it
// increases with each attempt the server schedules, including after a reset, which sets the attempt
// number back to 1.
type activityAttemptStampSuite struct {
	parallelsuite.Suite[*activityAttemptStampSuite]
}

func TestActivityAttemptStampSuite(t *testing.T) {
	parallelsuite.Run(t, &activityAttemptStampSuite{})
}

func (s *activityAttemptStampSuite) TestRetryIncreasesStamp() {
	f := newAttemptStampFixture(s.T())
	f.startOwner(f.tv)
	f.scheduleRetriedActivity(f.tv)
	attempt1 := f.pollActivityTask(f.tv)
	attempt2 := f.pollActivityTask(f.tv) // dispatched once attempt 1 times out
	require.Equal(s.T(), int32(2), attempt2.Attempt)
	require.Greater(s.T(), attempt2.AttemptStamp, attempt1.AttemptStamp)
}

// A reset between attempts sets the next attempt's number back to 1, but not its stamp. A reset
// during an attempt is not tested, because the two activity surfaces differ there; see
// TestAttemptBeforeResetIsFencedAfterSuccessorAppends.
func (s *activityAttemptStampSuite) TestResetBetweenAttemptsIncreasesStamp() {
	f := newAttemptStampFixture(s.T())
	f.startOwner(f.tv)
	f.scheduleRetriedActivity(f.tv)
	attempt1 := f.pollActivityTask(f.tv)
	f.resetActivityAwaitingAttempt(f.tv, 2)
	afterReset := f.pollActivityTask(f.tv)
	require.Equal(s.T(), int32(1), afterReset.Attempt)
	require.Greater(s.T(), afterReset.AttemptStamp, attempt1.AttemptStamp)
}

func newAttemptStampFixture(t *testing.T) *streamFixture {
	env := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.EnableActivityRetryStampIncrement, true),
	)
	return &streamFixture{t: t, env: env, tv: env.Tv()}
}
