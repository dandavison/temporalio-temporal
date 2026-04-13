#!/usr/bin/env bash
# Convert all .d2 files to high-res PNGs with transparent backgrounds.
# d2 natively produces RGBA PNGs when fill is set to transparent.
#
# Usage: ./build-pngs.sh [file.d2 ...]
# If no arguments, converts all .d2 files in the script's directory.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCALE=3  # 3x for crisp slides

if [ $# -gt 0 ]; then
    files=("$@")
else
    files=("$SCRIPT_DIR"/*.d2)
fi

for d2_file in "${files[@]}"; do
    [ -f "$d2_file" ] || continue
    base="$(basename "$d2_file" .d2)"
    dir="$(dirname "$d2_file")"

    echo "Building $base..."
    (
        cd "$dir"
        d2 --scale "$SCALE" "$base.d2" "$base.png"
    )
    echo "  -> $dir/$base.png"
done
