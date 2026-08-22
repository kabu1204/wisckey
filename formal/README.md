# WiscKey maintenance model

This directory contains the bounded Z3 model used to justify two-way value-log
garbage collection. It is a CI/development tool and is not linked into the
LevelDB library.

The checked domain contains two keys, two GC jobs, two source vlog files, two
replacement files, a checked 20-step horizon, and an enforced maximum of 24
atomic transitions. Twenty steps cover both complete GC phase sequences plus
foreground, snapshot, durability, and crash actions. Transitions cover source
reservation, copy, replacement sync, exact-handle LSM update, LSM sync,
manifest publication, snapshot-safe reclaim, foreground put/delete, snapshot
lifecycle, and crash recovery.

The proof assumes the implementation continues to serialize vlog manifest
updates and uses the complete old `ValueHandle` in the GC write callback. Z3
infers `distinct_source` as the minimal pairwise scheduling guard. The matching
C++ predicate is `CanRunVLogGCConcurrently` in `db/blob_vlog_impl.h`.

Run all checks from the repository root:

```sh
uv sync --project formal --frozen
uv run --directory formal pytest
uv run --directory formal ruff check .
uv run --directory formal python -m maintenance.check
```

Pass `--trace-dir <temporary-directory>` to write stable JSON counterexample
traces for the four negative controls. The model is bounded and does not claim
to prove more than two concurrent GC jobs or any parallel LSM compaction.
