#!/usr/bin/env bash
# Run from the repository root: ./tests/repro_activity_timeout_driver.sh

set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

stdout_file=$(mktemp)
stderr_file=$(mktemp)
cleanup() {
	rm -f -- "$stdout_file" "$stderr_file"
}
trap cleanup EXIT

command=(go test -tags test_dep,integration ./tests -run '^TestActivityTimeoutInfoRejectsUnrelatedScheduleToClose$' -count=1)
if "${command[@]}" >"$stdout_file" 2>"$stderr_file"; then
	exit_code=0
else
	exit_code=$?
fi

printf '# Activity timeout driver false positive\n\n'
printf '## Scenario\n\n'
printf 'The driver is waiting for a start-to-close timeout, but the observed terminal timeout is schedule-to-close. These are distinct timeout events, so the driver should reject the observation.\n\n'
printf '```bash\n%s\n```\n\n' "${command[*]}"
printf '### stdout\n\n```\n'
sed -n '1,200p' "$stdout_file"
printf '```\n\n### stderr\n\n```\n'
sed -n '1,200p' "$stderr_file"
printf '```\n\nExit code: `%d`\n\n' "$exit_code"
printf '## Finding\n\n'
if ((exit_code == 0)); then
	printf 'The bug was not reproduced: the driver rejected the unrelated timeout.\n'
	exit 1
fi
printf 'The assertion failed because the driver accepted schedule-to-close as evidence that start-to-close fired, demonstrating the false positive.\n'
