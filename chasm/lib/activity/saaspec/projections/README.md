# saaspec projections

*Projections* are read-only, spec-derived artifacts for humans. Each is a deterministic pure
function of the SAA behavior spec: every status, edge, timer, rejection, and per-operation
classification is obtained by executing `saaspec.Model` / `Initial` / `ExpectedDescribe` /
`ExpectedHeartbeatFlags`, funneled through the shared view-model in [`lifecycle/`](lifecycle/). The
renderers author only presentation (phrasing, colors, layout, timeline geometry) — **no product
behavior and no access to the implementation** — so a projection cannot drift from the spec.

Live gallery: https://dandavison.github.io/etc/saa/

## The generators

All live under [`cmd/`](cmd/) and run from the repository root. Each prints to stdout; `-o` writes a
file.

| Generator | Emits | Consumed by |
|-----------|-------|-------------|
| `saadiagram` | d2 source | the lifecycle diagram (`lifecycle.d2` → `lifecycle.svg`) |
| `saaprose` | Markdown | the docs site (`activity-operations.generated.md`) |
| `saaanim` | a TypeScript data module | the canvas-commons state-machine animation |
| `saatimelines` | a TypeScript data module | the canvas-commons per-trace timelines animation |
| `saainteractive` | a JS/JSON data table | the interactive explorer page |

## Committed in-repo artifacts

Two artifacts are checked in and verified in CI; regenerate them with `make`:

```bash
make saa-lifecycle-diagram         # -> lifecycle.d2 (+ lifecycle.svg if d2 is installed)
make saa-activity-operations-doc   # -> activity-operations.generated.md (splices into the docs repo)
```

## Regenerating the hosted pages

The gallery lives in the **`dandavison/etc`** repo under `saa/` (that repo owns the
`dandavison.github.io/etc/` URL prefix). Each page is a hand-authored `index.html` plus generated
assets; the commands below rebuild the assets. They reference two external checkouts:

```bash
SITE=/path/to/dandavison-etc/saa                       # the gh-pages content
CC=/path/to/canvas-commons                             # animation framework checkout
CAP=~/.claude/skills/canvas-commons-animations/scripts/capture-frames.mjs
GEN=./chasm/lib/activity/saaspec/projections/cmd       # run from the temporal repo root
```

### `diagram/` — lifecycle d2 table

```bash
make saa-lifecycle-diagram
cp chasm/lib/activity/saaspec/projections/lifecycle.svg "$SITE/lifecycle.svg"
cp chasm/lib/activity/saaspec/projections/lifecycle.svg "$SITE/diagram/preview.svg"
```

### `prose/` — docs Markdown (the page fetches `../operations.md` and renders it)

```bash
make saa-activity-operations-doc
cp chasm/lib/activity/saaspec/projections/activity-operations.generated.md "$SITE/operations.md"
```

### `explorer/` — interactive page (static; looks up the precomputed table)

```bash
go run "$GEN/saainteractive" -o "$SITE/explorer/spec-data.js" -json "$SITE/explorer/spec-data.json"
```

### `state-machine/` — canvas-commons state-graph animation (`saa.tsx`)

```bash
go run "$GEN/saaanim" -o "$CC/packages/template/src/scenes/saaspec-data.ts"
( cd "$CC" && pnpm template:build \
  && node "$CAP" --project-dir packages/template --all --scale 1 --encode saa.mp4 )
cp "$CC/saa.mp4" "$SITE/state-machine/saa.mp4"
# stills: capture a few fractions and copy them in as frame-1..N.png (frame-N.png is the poster)
( cd "$CC" && node "$CAP" --project-dir packages/template --fractions 0,0.25,0.5,0.75,1 )
```

### `timelines/` — canvas-commons per-trace timelines (`saa-timelines.tsx`)

Register the timelines scene in `packages/template/src/project.ts` (swap `saa` for `saa-timelines`)
before building, then:

```bash
go run "$GEN/saatimelines" -o "$CC/packages/template/src/scenes/saa-timelines-data.ts"
( cd "$CC" && pnpm template:build \
  && node "$CAP" --project-dir packages/template --fractions 0,0.2,0.4,0.6,0.8,1 )
# copy the stills in as the per-trace frame-*.png the page references
```

After refreshing assets, commit and push from the `dandavison/etc` checkout (its `master` is served).
