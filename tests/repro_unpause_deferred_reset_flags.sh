#!/usr/bin/env bash
# Run: ./tests/repro_unpause_deferred_reset_flags.sh

set -euo pipefail

repro_tmp="$(mktemp -d)"
trap 'rm -rf "${repro_tmp}"' EXIT

command="go test -count=1 -tags test_dep -run 'TestActivityParityTestSuite/TestUnpause_Declarative' ./tests/"
if TEMPORAL_TEST_LOG_LEVEL=ERROR TEMPORAL_TEST_LOG_STACKTRACE_LEVEL=off \
	go test -count=1 -tags test_dep \
	-run 'TestActivityParityTestSuite/TestUnpause_Declarative' \
	./tests/ >"${repro_tmp}/stdout" 2>"${repro_tmp}/stderr"; then
	exit_code=0
else
	exit_code=$?
fi

if ((exit_code == 0)); then
	finding="The bug was not reproduced: both scenarios produced attempt 1 with no heartbeat checkpoint."
else
	finding="With retries remaining, the server records the next attempt as 3 and preserves the heartbeat
checkpoint. At the maximum attempt, it fails terminally instead of reopening retries at attempt 1."
fi

cat <<EOF
# Unpause drops deferred activity reset flags

## Scenarios

Attempt 2 is heartbeated and paused while its worker is still running. Unpause requests both
\`reset_attempts\` and \`reset_heartbeat\`; after the worker fails retryably, the next task should be
attempt 1 with no heartbeat checkpoint. The trace runs both with retries remaining and with attempt 2
at the configured maximum.

\`\`\`bash
export TEMPORAL_TEST_LOG_LEVEL=ERROR TEMPORAL_TEST_LOG_STACKTRACE_LEVEL=off
${command}
\`\`\`

### stdout

\`\`\`text
$(<"${repro_tmp}/stdout")
\`\`\`

### stderr

\`\`\`text
$(<"${repro_tmp}/stderr")
\`\`\`

Exit code: ${exit_code}

## Finding

${finding}
EOF
