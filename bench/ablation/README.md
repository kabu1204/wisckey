# BlobDB sequential-read ablation

Branch: `perf/seq-read-fixes`

Investigates why this WiscKey-style KV-separation fork is slow on sequential
read workloads (`readseq` / `readreverse`).

## Setup

```
CPU:    4 vCPU
MEM:    11 GB
DISK:   overlay fs (mmap-default env)
```

- Built with `cmake -DCMAKE_BUILD_TYPE=Release`.
- `db_bench`, `--blob=1 --blob_prefetch=0 --blob_iter_threads=0`.
- Two value sizes: 128B (1M entries, 1MB vlog files) and 1KB (500K entries,
  4MB vlog files). These create multiple read-only `.vlog` files so the
  `ro_files_`/cache path is exercised.
- Each workload repeated 3x; **best** (max MB/s) is reported.
- Data fits in the page cache (cache-warm), so this measures **CPU / lock /
  allocation** overhead, not disk I/O. This is exactly where the fork's
  small-value readseq regression lives.

## Configs

| config     | commit   | change |
|------------|----------|--------|
| baseline   | 9d5954d  | fork HEAD, unchanged |
| fixA       | 079eca9  | `VLogReader::ReadValue` direct read in `Get`; drop per-lookup `VLogReaderIterator` alloc |
| fixAB       | 4c692b4  | fixA + read-ahead buffer in `VLogReaderIterator::ParseCurrentEntry` |

A `vlogscan` workload (`BlobDB::ScanVLog`) was added in fixA to measure raw
value-log sequential scan throughput in isolation (no LSM probes). It is
absent from baseline.

## Results

Read throughput (MB/s, best of 3, cache-warm):

| workload      | baseline 128 | fixA 128 | fixAB 128 | baseline 1K | fixA 1K | fixAB 1K |
|---------------|-------------|----------|-----------|-------------|---------|----------|
| readseq       | 479.3       | 739.1    | 797.0     | 2399.5      | 3724.8  | 3122.5   |
| readreverse   | 338.2       | 494.6    | 493.8     | 1362.9      | 1963.7  | 1872.3   |
| readrandom    | 55.5        | 59.4     | 61.8      | 367.8       | 400.1   | 349.0    |
| vlogscan      | n/a         | 4755.9   | 4556.8    | n/a         | 32697.6 | 12909.7  |

Speedup vs baseline (128B):

| workload    | fixA  | fixAB |
|-------------|-------|-------|
| readseq     | 1.54x | 1.66x |
| readreverse | 1.46x | 1.46x |
| readrandom  | 1.07x | 1.11x |

## Findings

### Fix A: direct value read — effective (kept)

The hot point-lookup path (`ValueLogImpl::Get` -> `GetLocked`) built and
destroyed a `VLogReaderIterator` (plus a `char[1024]` scratch) on **every
single Get**, then `Seek`'d it, only to read one record whose offset+size were
already known from the `ValueHandle`. On sequential reads the LSM iterator
issues one vlog `Get` per entry, so this per-entry allocation dominated.

Adding `VLogReader::ReadValue(handle)` that reads the record directly with a
small stack buffer (heap fallback for large values) and parses the two varints
in place yields:

- `readseq` 128B: **1.54x** (479 -> 739 MB/s)
- `readseq` 1K:   **1.55x** (2399 -> 3725 MB/s)
- `readreverse` similar (~1.46x)
- `readrandom`: marginal (within noise), as expected — random reads were
  already LSM-bound, and the per-Get alloc was a smaller share.

### Fix B: read-ahead buffer — NOT effective under mmap (reverted)

`VLogReaderIterator::ParseCurrentEntry`'s sequential path read each record
with two separate reads (10-byte header, then key+value). An 8KB read-ahead
window would batch these for a `pread`-based backend.

The ablation **disproved** this hypothesis for the default environment: the
posix env serves vlog reads via `mmap` (`PosixMmapAppendableFile` /
`PosixMmapReadableFile`), where `Read()` returns a **zero-copy slice** into
mapped memory. There is no per-read syscall to amortize, so the "two reads"
were just two cheap slice constructions. The read-ahead buffer instead added
an unconditional 8KB `memcpy` into `ra_buf_` that the mmap path never needed:

- `vlogscan` 1K: fixA 32697 MB/s -> fixAB 12909 MB/s (**2.5x slower**)
- `vlogscan` 128B: ~flat (records tiny, copy cost ~equal)
- `readseq` 1K also regressed (3724 -> 3122 MB/s).

Fix B was **reverted** (`f6c4b32`). It would only help a `pread`-based file
backend (or cold, disk-bound data larger than RAM), neither of which the
default posix env exercises here.

### What this does NOT address

- The fundamental WiscKey sequential-read issue (LSM-order scan issuing one
  vlog `Get` per entry) is improved per-entry but not replaced. The
  paper-recommended "scan vlog in large blocks + LSM validity join" path is
  still absent. Fix A makes the per-entry cost cheap enough that, cache-warm,
  the gap to a native sequential scan is much smaller.
- Small-value `fillseq` (128B: 41.5 -> 45.4 MB/s) is barely affected — the
  vlog write is still serialized under `rwlock_` write-lock, separate from
  LSM BatchGroup. That's a write-path issue.
- GC `Collect` still does a full sequential scan + per-record LSM `Get`. The
  added `vlogscan` workload exposes the raw scan cost; a GC-specific
  workload would need LSM-overwrite setup.

## Reproducing

```bash
# build a config at a given ref
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j4 --target db_bench
# run
./build/db_bench --db=/tmp/db --num=1000000 --value_size=128 --bloom_bits=8 \
  --blob=1 --blob_prefetch=0 --blob_iter_threads=0 --blob_value_size_threshold=64 \
  --blob_file_size=1048576 --benchmarks=fillseq,readseq,readseq,readseq,readreverse,readreverse,readreverse,readrandom,readrandom,readrandom,vlogscan,vlogscan,vlogscan,fillrandom,overwrite

# regenerate plots from bench/ablation/data/*.log
python3 bench/ablation/plot.py
```

Artifacts: `bench/ablation/data/*.log`, `bench/ablation/results.csv`,
`bench/ablation/*.png`.
