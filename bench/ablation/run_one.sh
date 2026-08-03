#!/usr/bin/env bash
# Ablation harness: build a given git ref and run the canonical BlobDB
# benchmark for two value sizes, saving raw db_bench output.
#
# Usage: ./run_one.sh <label> <git-ref> <builddir>
set -euo pipefail

LABEL="$1"
REF="$2"
BLD="$3"
ROOT="$(git rev-parse --show-toplevel)"
DATA="$ROOT/bench/ablation/data"
mkdir -p "$DATA"

cd "$ROOT"
git checkout -q "$REF"

if [ ! -d "$BLD" ]; then
  cmake -S . -B "$BLD" -DCMAKE_BUILD_TYPE=Release -DLEVELDB_BUILD_TESTS=ON >/dev/null 2>&1
fi
cmake --build "$BLD" -j 4 --target db_bench >/dev/null 2>&1

DBD="/tmp/agent_15bc0092-a48d-4e4b-937b-cb3ac1daaac5/ablate/$LABEL"
BENCH="$BLD/db_bench"

# vlogscan exists only on fixA / fixAB (added by the perf commits)
VLOGSCAN=""
if [ "$LABEL" != "baseline" ]; then
  VLOGSCAN=",vlogscan,vlogscan,vlogscan"
fi
BENCHES="fillseq,readseq,readseq,readseq,readreverse,readreverse,readreverse,readrandom,readrandom,readrandom${VLOGSCAN},fillrandom,overwrite"

run() {
  local size="$1" num="$2" fsize="$3" thr="$4"
  rm -rf "$DBD"
  echo "=== $LABEL value_size=$size ===" >&2
  "$BENCH" --db="$DBD" --num="$num" --value_size="$size" --bloom_bits=8 \
    --blob=1 --blob_prefetch=0 --blob_iter_threads=0 \
    --blob_value_size_threshold="$thr" --blob_file_size="$fsize" \
    --benchmarks="$BENCHES" 2>/dev/null \
    | grep -E "^(fillseq|fillrandom|overwrite|readseq|readreverse|readrandom|vlogscan)" \
    > "$DATA/${LABEL}_${size}.log"
}

# 128B: 1M entries, 1MB vlog files (~14 read-only files)
run 128 1000000 1048576 64
# 1KB: 500K entries, 4MB vlog files (~125 read-only files)
run 1024 500000 4194304 128

git checkout -q perf/seq-read-fixes
echo "done $LABEL" >&2
