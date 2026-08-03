#!/usr/bin/env python3
"""Parse db_bench ablation logs and emit CSV + plots.

Takes the BEST (max MB/s) of repeated runs of each workload.
"""
import csv
import glob
import os
import re

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

DATA = os.path.join(os.path.dirname(__file__), "data")
OUT = os.path.join(os.path.dirname(__file__))

LINE = re.compile(
    r"^(?P<name>\w+)\s+:\s+(?P<us>[\d.]+)\s+micros/op;\s+(?P<mbs>[\d.]+)\s+MB/s"
)

CONFIGS = ["baseline", "fixA", "fixAB"]
SIZES = ["128", "1024"]
# workloads compared across configs (Fix A effect); readseq/readreverse/readrandom
READ_WS = ["readseq", "readreverse", "readrandom"]
WRITE_WS = ["fillseq", "fillrandom", "overwrite"]


def best_mbs(path, name):
    """Return the best (max) MB/s for a given workload in a log file."""
    best = None
    if not os.path.exists(path):
        return None
    with open(path) as f:
        for line in f:
            m = LINE.match(line.strip())
            if not m:
                continue
            if m.group("name") != name:
                continue
            mbs = float(m.group("mbs"))
            if best is None or mbs > best:
                best = mbs
    return best


def parse_all():
    rows = []
    for cfg in CONFIGS:
        for size in SIZES:
            log = os.path.join(DATA, f"{cfg}_{size}.log")
            for w in READ_WS + WRITE_WS + (["vlogscan"] if cfg != "baseline" else []):
                rows.append(
                    {
                        "config": cfg,
                        "value_size": size,
                        "workload": w,
                        "mbs": best_mbs(log, w),
                    }
                )
    return rows


def main():
    rows = parse_all()
    # write CSV
    with open(os.path.join(OUT, "results.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["config", "value_size", "workload", "mbs"])
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print("wrote results.csv")

    # ---------- Plot 1: read throughput (Fix A effect) ----------
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    for ax, size in zip(axes, SIZES):
        x = range(len(READ_WS))
        width = 0.25
        for i, cfg in enumerate(CONFIGS):
            vals = [best_mbs(os.path.join(DATA, f"{cfg}_{size}.log"), w) or 0
                    for w in READ_WS]
            bars = ax.bar([xi + i * width for xi in x], vals, width, label=cfg)
            for b, v in zip(bars, vals):
                if v:
                    ax.text(b.get_x() + b.get_width() / 2, v, f"{v:.0f}",
                            ha="center", va="bottom", fontsize=7)
        ax.set_xticks([xi + width for xi in x])
        ax.set_xticklabels(READ_WS)
        ax.set_ylabel("Throughput (MB/s, higher is better)")
        ax.set_title(f"value_size = {size}B")
        ax.legend()
        ax.grid(axis="y", alpha=0.3)
    fig.suptitle("BlobDB read-path ablation: Fix A (direct value read in Get)\n"
                 "best of 3 repeats; cache-warm, mmap", y=1.02)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "read_throughput.png"), dpi=120)
    print("wrote read_throughput.png")

    # ---------- Plot 2: speedup ratios vs baseline ----------
    fig, ax = plt.subplots(figsize=(8, 5))
    ws_labels = READ_WS
    x = range(len(ws_labels))
    width = 0.35
    for i, (cfg, color) in enumerate(zip(["fixA", "fixAB"], ["#2ca02c", "#d62728"])):
        ratios = []
        for w in READ_WS:
            base = best_mbs(os.path.join(DATA, f"baseline_128.log"), w) or 1
            cur = best_mbs(os.path.join(DATA, f"{cfg}_128.log"), w) or 1
            ratios.append(cur / base)
        ax.bar([xi + i * width for xi in x], ratios, width, label=cfg, color=color)
    ax.axhline(1.0, color="black", linewidth=0.8, linestyle="--")
    ax.set_xticks([xi + width / 2 for xi in x])
    ax.set_xticklabels(ws_labels)
    ax.set_ylabel("Speedup vs baseline (x)")
    ax.set_title("128B read-path speedup (baseline = 1.0)")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "speedup_128.png"), dpi=120)
    print("wrote speedup_128.png")

    # ---------- Plot 3: vlogscan (Fix B effect / non-effect) ----------
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
    for ax, size in zip(axes, SIZES):
        cfgs = ["fixA", "fixAB"]
        vals = [best_mbs(os.path.join(DATA, f"{c}_{size}.log"), "vlogscan") or 0
                for c in cfgs]
        bars = ax.bar(cfgs, vals, color=["#2ca02c", "#d62728"])
        for b, v in zip(bars, vals):
            ax.text(b.get_x() + b.get_width() / 2, v, f"{v:.0f}",
                    ha="center", va="bottom", fontsize=9)
        ax.set_ylabel("Scan throughput (MB/s)")
        ax.set_title(f"value_size = {size}B")
        ax.grid(axis="y", alpha=0.3)
    fig.suptitle("vlog sequential-scan: Fix B (read-ahead) is neutral/negative "
                 "under mmap\n(reads are zero-copy slices; read-ahead adds an "
                 "extra copy) -> reverted", y=1.03)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "vlogscan.png"), dpi=120)
    print("wrote vlogscan.png")


if __name__ == "__main__":
    main()
