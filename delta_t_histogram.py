"""
Usage:
    python delta_t_histogram.py
"""

import argparse
import logging
import multiprocessing as mp
import time

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from pymongo import MongoClient

from config import MONGO_URI, DB_NAME, CLEAN_COL, NUM_WORKERS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# -- Worker -------------------------------------------------------------------

def worker_delta_t(args: tuple) -> list[float]:
    mmsi_batch, uri, db_name, clean_col, worker_id = args

    client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    col    = client[db_name][clean_col]

    pipeline = [
        # Filter to this worker's vessel batch
        {"$match": {"mmsi": {"$in": mmsi_batch}}},

        # Compute previous timestamp within each vessel's ping sequence
        {"$setWindowFields": {
            "partitionBy": "$mmsi",
            "sortBy":      {"ts": 1},
            "output": {
                "prev_ts": {
                    "$shift": {"output": "$ts", "by": -1}
                }
            }
        }},

        # Drop first ping per vessel (no previous timestamp)
        {"$match": {"prev_ts": {"$ne": None}}},

        # Compute delta-t in milliseconds inside MongoDB
        {"$project": {
            "_id": 0,
            "dt_ms": {
                "$dateDiff": {
                    "startDate": "$prev_ts",
                    "endDate":   "$ts",
                    "unit":      "millisecond",
                }
            }
        }},

        # Drop zero or negative deltas (duplicate timestamps)
        {"$match": {"dt_ms": {"$gt": 0}}},
    ]

    results = list(col.aggregate(pipeline, allowDiskUse=True))
    deltas  = [r["dt_ms"] for r in results]

    client.close()
    log.info("Worker %d: %d vessels, %d delta-t values",
             worker_id, len(mmsi_batch), len(deltas))
    return deltas


# -- Plotting -----------------------------------------------------------------

def save_histogram(arr: np.ndarray, filename: str, title: str,
                   color: str, median_ms: float, mean_ms: float,
                   p95_ms: float = None, max_s: float = None) -> None:
    fig, ax = plt.subplots(figsize=(10, 6))

    data = arr if max_s is None else arr[arr <= max_s * 1000]
    ax.hist(data / 1000, bins=150, color=color, edgecolor="none", alpha=0.85)

    ax.axvline(median_ms / 1000, color="red",    linestyle="--", linewidth=1.5,
               label=f"Median {median_ms/1000:.1f} s")
    ax.axvline(mean_ms   / 1000, color="orange", linestyle="--", linewidth=1.5,
               label=f"Mean {mean_ms/1000:.1f} s")
    if p95_ms is not None:
        ax.axvline(p95_ms / 1000, color="gray", linestyle=":", linewidth=1.2,
                   label=f"95th pct {p95_ms/1000:.1f} s")

    ax.set_xlabel("Delta-t (seconds)")
    ax.set_ylabel("Number of ping pairs")
    ax.set_title(title)
    ax.legend(fontsize=9)
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", linewidth=0.4, alpha=0.5)
    plt.tight_layout()
    plt.savefig(filename, dpi=150, bbox_inches="tight")
    plt.close()
    log.info("Saved -> %s", filename)


# -- Main ---------------------------------------------------------------------

def run(uri: str, db_name: str, clean_col: str,
        num_workers: int, batch_size: int, output: str) -> None:

    t0 = time.perf_counter()

    client = MongoClient(uri)
    col    = client[db_name][clean_col]
    log.info("Fetching distinct MMSIs from %s...", clean_col)
    mmsis  = col.distinct("mmsi")
    client.close()
    log.info("Found %d vessels", len(mmsis))

    batches = [mmsis[i:i + batch_size]
               for i in range(0, len(mmsis), batch_size)]
    worker_args = [
        (batch, uri, db_name, clean_col, i % num_workers)
        for i, batch in enumerate(batches)
    ]

    log.info("Computing delta-t in parallel with %d workers...", num_workers)
    all_deltas = []
    done       = 0
    total      = len(batches)
    log_every  = max(1, total // 10)

    with mp.Pool(processes=num_workers) as pool:
        for result in pool.imap_unordered(worker_delta_t, worker_args):
            all_deltas.extend(result)
            done += 1
            if done % log_every == 0 or done == total:
                log.info("Progress: %d/%d batches", done, total)

    log.info("Total delta-t values: %d", len(all_deltas))

    arr       = np.array(all_deltas)
    median_ms = float(np.median(arr))
    mean_ms   = float(np.mean(arr))
    p95_ms    = float(np.percentile(arr, 95))

    log.info("Median: %.0f ms (%.1f s)", median_ms, median_ms / 1000)
    log.info("Mean:   %.0f ms (%.1f s)", mean_ms,   mean_ms   / 1000)
    log.info("95th:   %.0f ms (%.1f s)", p95_ms,    p95_ms    / 1000)

    base = output.replace(".png", "")

    save_histogram(arr, f"{base}_full.png",
                   "AIS Ping Interval — Full Distribution (no cap)",
                   "#1D9E75", median_ms, mean_ms)

    save_histogram(arr, f"{base}_zoomed.png",
                   "AIS Ping Interval — Zoomed 0-60 seconds",
                   "#E85D26", median_ms, mean_ms, max_s=60)

    elapsed = time.perf_counter() - t0
    log.info("Total time: %.1f s", elapsed)

    print("\n=== Delta-t Analysis Summary ===")
    print(f"Total ping pairs:   {len(all_deltas):,}")
    print(f"Median interval:    {median_ms/1000:.1f} s  ({median_ms:.0f} ms)")
    print(f"Mean interval:      {mean_ms/1000:.1f} s  ({mean_ms:.0f} ms)")
    print(f"95th percentile:    {p95_ms/1000:.1f} s  ({p95_ms:.0f} ms)")
    print(f"Max interval:       {arr.max()/1000:.1f} s")
    print(f"Min interval:       {arr.min():.0f} ms")


# -- Entry point --------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri",        default=MONGO_URI)
    parser.add_argument("--db",         default=DB_NAME)
    parser.add_argument("--collection", default=CLEAN_COL)
    parser.add_argument("--workers",    type=int,
                        default=NUM_WORKERS)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--output",     default="delta_t_histogram.png")
    args = parser.parse_args()

    run(
        uri         = args.uri,
        db_name     = args.db,
        clean_col   = args.collection,
        num_workers = args.workers,
        batch_size  = args.batch_size,
        output      = args.output,
    )