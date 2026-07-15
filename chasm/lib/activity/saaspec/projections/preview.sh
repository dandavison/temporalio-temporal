#!/usr/bin/env bash
# Build the locally-renderable SAA projections into a temp dir and print the gallery's index.html.
#
# Covers the three projections that are self-contained (need only Go, and d2 for the diagram):
# the lifecycle diagram, the docs prose, and the interactive explorer. The two canvas-commons
# animations (state-machine, timelines) need an external framework checkout to render, so the
# gallery links out to the live versions for those. Everything is derived from saaspec.Model.
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../.." && pwd)
proj=chasm/lib/activity/saaspec/projections
pages="$repo/$proj/pages"
live=https://dandavison.github.io/etc/saa

out=$(mktemp -d "${TMPDIR:-/tmp}/saa-projections.XXXXXX")
mkdir -p "$out/diagram" "$out/prose" "$out/explorer"

cd "$repo"

# diagram: d2 source -> svg (fall back to the committed svg if d2 isn't installed).
go run "./$proj/cmd/saadiagram" > "$out/lifecycle.d2"
if command -v d2 >/dev/null 2>&1; then
  d2 "$out/lifecycle.d2" "$out/lifecycle.svg" >/dev/null 2>&1
else
  cp "$repo/$proj/lifecycle.svg" "$out/lifecycle.svg"
  echo "note: d2 not installed; using the committed lifecycle.svg" >&2
fi
cp "$pages/diagram.html" "$out/diagram/index.html"

# prose: markdown, inlined into the page so it renders over file:// (no fetch).
go run "./$proj/cmd/saaprose" > "$out/operations.md"
awk -v f="$out/operations.md" '
  /@@MARKDOWN@@/ { while ((getline line < f) > 0) print line; next }
  { print }
' "$pages/prose.html" > "$out/prose/index.html"

# explorer: precomputed lookup table assigned to window.SAA_SPEC.
go run "./$proj/cmd/saainteractive" -o "$out/explorer/spec-data.js" >/dev/null
cp "$pages/explorer.html" "$out/explorer/index.html"

cat > "$out/index.html" <<HTML
<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Standalone Activity — behavior projections (local)</title>
<style>
  body{font:16px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;max-width:820px;margin:2.2rem auto;padding:0 1rem;color:#1c2024}
  h1{font-size:1.6rem;margin:0 0 .3rem} .lead{color:#606770;margin:0 0 1.6rem}
  ul{line-height:2} a{color:#2d6cdf} .ext{color:#606770;font-size:.9rem}
  code{background:#f2f4f7;padding:.1em .35em;border-radius:4px;font-size:.9em}
</style></head><body>
<h1>Standalone Activity — behavior projections</h1>
<p class="lead">Generated locally from <code>saaspec.Model</code>. Every value below is derived from
the spec; none is hand-authored.</p>
<ul>
  <li><a href="diagram/">Lifecycle &amp; operations diagram</a> — what each operation does in each phase</li>
  <li><a href="prose/">Docs prose</a> — the Markdown generated for the docs site</li>
  <li><a href="explorer/">Interactive explorer</a> — send operations, see allowed / deferred / not permitted up-front</li>
  <li class="ext">State-machine animation and animated timelines need the canvas-commons
      checkout to render — see them live at <a href="$live/">$live/</a></li>
</ul>
</body></html>
HTML

echo "$out/index.html"
