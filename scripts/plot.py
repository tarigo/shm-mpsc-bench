#!/usr/bin/env python3
"""
Render benchmark plots from bench JSON output.

Usage:
    python3 scripts/plot.py out/bench.json out/

Reads a JSON array of {codec, scenario, events_per_sec, wire_mb_per_sec,
payload_mb_per_sec, avg_slot_bytes, framing_overhead_pct, posted_per_1k, ...}
records (the format `bench --out` produces) and writes PNG plots.
"""
from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

SCENARIO_ORDER = ["small", "medium", "large", "huge"]
CODEC_ORDER = ["capnp", "json", "cbor", "proto", "flat", "avro"]
CODEC_COLORS = {
    "capnp": "#2e86c1",
    "json":  "#e67e22",
    "cbor":  "#28b463",
    "proto": "#8e44ad",
    "flat":  "#c0392b",
    "avro":  "#7f8c8d",
}


def load(path: Path) -> list[dict]:
    with open(path) as f:
        return json.load(f)


def group_by_scenario(rows: list[dict]) -> dict[str, dict[str, dict]]:
    out: dict[str, dict[str, dict]] = defaultdict(dict)
    for r in rows:
        out[r["scenario"]][r["codec"]] = r
    return out


def grouped_bars(ax, scenarios, codecs, values, ylabel, title, log=False):
    n_codecs = len(codecs)
    n_scen = len(scenarios)
    width = 0.8 / n_codecs
    x = list(range(n_scen))
    for i, codec in enumerate(codecs):
        ys = [values[s].get(codec, 0.0) for s in scenarios]
        offsets = [xx + (i - (n_codecs - 1) / 2) * width for xx in x]
        ax.bar(offsets, ys, width=width, label=codec, color=CODEC_COLORS.get(codec))
    ax.set_xticks(x)
    ax.set_xticklabels(scenarios)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    if log:
        ax.set_yscale("log")
    ax.grid(True, axis="y", alpha=0.3)
    ax.legend(ncols=3, fontsize=8, loc="best")


def plot_metric(rows, scenarios, codecs, metric, ylabel, title, out_path, log=False):
    values: dict[str, dict[str, float]] = {s: {} for s in scenarios}
    for r in rows:
        if r["scenario"] in values:
            values[r["scenario"]][r["codec"]] = float(r[metric])
    fig, ax = plt.subplots(figsize=(9, 5))
    grouped_bars(ax, scenarios, codecs, values, ylabel, title, log=log)
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)
    print(f"  wrote {out_path}")


def plot_summary(rows, scenarios, codecs, out_path):
    fig, axs = plt.subplots(2, 2, figsize=(13, 8))
    metrics = [
        ("events_per_sec", "events/s", "Throughput", True),
        ("wire_mb_per_sec", "wire MB/s", "Wire bandwidth", True),
        ("framing_overhead_pct", "overhead, %", "Framing overhead", False),
        ("avg_slot_bytes", "avg slot bytes", "Slot size on wire", True),
    ]
    for ax, (metric, ylabel, title, log) in zip(axs.flat, metrics):
        values: dict[str, dict[str, float]] = {s: {} for s in scenarios}
        for r in rows:
            if r["scenario"] in values:
                values[r["scenario"]][r["codec"]] = float(r[metric])
        grouped_bars(ax, scenarios, codecs, values, ylabel, title, log=log)
    fig.suptitle("shm-mpsc-bench summary", fontsize=14)
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)
    print(f"  wrote {out_path}")


def main(argv: list[str]) -> int:
    if len(argv) < 3:
        print(f"usage: {argv[0]} <bench.json> <out_dir>", file=sys.stderr)
        return 2
    in_path = Path(argv[1])
    out_dir = Path(argv[2])
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = load(in_path)
    scenarios = [s for s in SCENARIO_ORDER if any(r["scenario"] == s for r in rows)]
    codecs = [c for c in CODEC_ORDER if any(r["codec"] == c for r in rows)]

    plot_metric(
        rows, scenarios, codecs,
        "events_per_sec", "events/s",
        "End-to-end throughput",
        out_dir / "throughput.png",
        log=True,
    )
    plot_metric(
        rows, scenarios, codecs,
        "wire_mb_per_sec", "MB/s",
        "Wire bandwidth (consumer-side)",
        out_dir / "bandwidth.png",
        log=True,
    )
    plot_metric(
        rows, scenarios, codecs,
        "framing_overhead_pct", "%",
        "Framing overhead = 1 − payload / wire",
        out_dir / "overhead.png",
    )
    plot_metric(
        rows, scenarios, codecs,
        "avg_slot_bytes", "bytes",
        "Average slot size on wire",
        out_dir / "slot_bytes.png",
        log=True,
    )
    plot_summary(rows, scenarios, codecs, out_dir / "summary.png")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
