"""
Vessel must have >= 100 valid pings.
Every document in vessels_clean must have all required fields:
mmsi, lat, lon, sog, cog, heading, rot, nav_status.

Strategy:
  1. Create raw collection indexes.
  2. Find valid MMSIs in parallel.
  3. Reset vessels_clean.
  4. Shard vessels_clean by hashed mmsi if running through mongos.
  5. Process MMSI batches in parallel.
  6. Each worker keeps its own MongoClient.
  7. Each worker runs a MongoDB aggregation with $merge.
  8. Create indexes after data is written.

Usage:
    python filter_vessels.py
"""

import argparse
import logging
import multiprocessing as mp
import time

from pymongo import ASCENDING, MongoClient
from pymongo.errors import CollectionInvalid, OperationFailure

from config import MONGO_URI, DB_NAME, RAW_COL, CLEAN_COL, NUM_WORKERS, MIN_PINGS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

REQUIRED_FIELDS = [
    "mmsi",
    "lat",
    "lon",
    "sog",
    "cog",
    "heading",
    "rot",
    "nav_status",
]

DEFAULT_COUNT_INDEX_NAME = "valid_required_mmsi_partial"

_worker_client = None
_worker_db = None
_worker_raw_col = None
_worker_hint_name = None


# -- Shared helpers -----------------------------------------------------------

def required_field_conditions() -> dict:
    return {
        "mmsi": {"$type": "number"},
        "lat": {"$type": "number"},
        "lon": {"$type": "number"},
        "sog": {"$type": "number"},
        "cog": {"$type": "number"},
        "heading": {"$type": "number"},
        "rot": {"$type": "number"},
        "nav_status": {"$type": "string"},
    }


def aggregate_raw(pipeline: list, use_hint: bool = True) -> list:
    """
    Runs aggregation on vessels_raw.
    """
    kwargs = {"allowDiskUse": True}

    if use_hint and _worker_hint_name:
        kwargs["hint"] = _worker_hint_name

    try:
        return list(_worker_raw_col.aggregate(pipeline, **kwargs))

    except OperationFailure:
        if "hint" in kwargs:
            kwargs.pop("hint", None)
            return list(_worker_raw_col.aggregate(pipeline, **kwargs))
        raise


# -- Worker setup -------------------------------------------------------------

def _worker_init(
    uri: str,
    db_name: str,
    raw_col: str,
    hint_name: str | None,
) -> None:
    """
    Called once per worker process.

    Each worker owns one MongoClient.
    """
    global _worker_client, _worker_db, _worker_raw_col, _worker_hint_name

    _worker_client = MongoClient(
        uri,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        socketTimeoutMS=None,
        maxPoolSize=1,
    )
    _worker_db = _worker_client[db_name]
    _worker_raw_col = _worker_db[raw_col]
    _worker_hint_name = hint_name


# -- Raw indexes --------------------------------------------------------------

def index_exists(
    uri: str,
    db_name: str,
    col_name: str,
    index_name: str,
) -> bool:
    client = MongoClient(uri)
    col = client[db_name][col_name]

    try:
        indexes = col.index_information()
        return index_name in indexes
    finally:
        client.close()


def ensure_raw_indexes(
    uri: str,
    db_name: str,
    raw_col: str,
    count_index_name: str,
) -> None:
    """
    Creates indexes useful for filtering.

    Run this after load_to_mongo.py, not before import.
    """
    client = MongoClient(uri)
    col = client[db_name][raw_col]

    t0 = time.perf_counter()

    log.info("Creating raw collection indexes if missing...")

    try:
        col.create_index(
            [("mmsi", ASCENDING)],
            name=count_index_name,
            partialFilterExpression=required_field_conditions(),
        )
        log.info("Created or confirmed partial index: %s", count_index_name)

    except OperationFailure as e:
        log.warning("Could not create partial index %s: %s", count_index_name, e)

    try:
        col.create_index(
            [("mmsi", ASCENDING), ("ts", ASCENDING)],
            name="mmsi_ts",
        )
        log.info("Created or confirmed index: mmsi_ts")

    except OperationFailure as e:
        log.warning("Could not create index mmsi_ts: %s", e)

    try:
        col.create_index(
            [("ts", ASCENDING)],
            name="ts_only",
        )
        log.info("Created or confirmed index: ts_only")

    except OperationFailure as e:
        log.warning("Could not create index ts_only: %s", e)

    elapsed = time.perf_counter() - t0
    client.close()

    log.info("Raw index step finished in %.1f s", elapsed)


def choose_count_hint(
    uri: str,
    db_name: str,
    raw_col: str,
    count_index_name: str,
) -> str | None:
    if index_exists(uri, db_name, raw_col, count_index_name):
        log.info("Using count/filter hint: %s", count_index_name)
        return count_index_name

    log.info(
        "Index %s not found. Counting will run without explicit hint.",
        count_index_name,
    )
    return None


# -- Step 1: parallel valid count mode ----------------------------------------

def worker_count_valid_mmsis(args: tuple) -> dict:
    batch_id, mmsi_batch, min_pings = args

    t0 = time.perf_counter()

    pipeline = [
        {"$match": {"mmsi": {"$in": mmsi_batch}}},
        {"$match": required_field_conditions()},
        {
            "$group": {
                "_id": "$mmsi",
                "count": {"$sum": 1},
            }
        },
        {"$match": {"count": {"$gte": min_pings}}},
        {"$sort": {"_id": 1}},
    ]

    try:
        docs = aggregate_raw(pipeline, use_hint=True)
        mmsis = [d["_id"] for d in docs]

        status = "ok"
        error = None

    except OperationFailure as e:
        mmsis = []
        status = "error"
        error = str(e)

    elapsed = time.perf_counter() - t0

    return {
        "batch_id": batch_id,
        "input_mmsis": len(mmsi_batch),
        "valid_mmsis": len(mmsis),
        "mmsis": mmsis,
        "status": status,
        "error": error,
        "elapsed_s": elapsed,
    }


def find_valid_mmsis_parallel(
    uri: str,
    db_name: str,
    raw_col: str,
    min_pings: int,
    num_workers: int,
    count_batch_size: int,
    hint_name: str | None,
) -> list[int]:
    """
    Counts only pings with all required fields.
    The counting stage is split by MMSI batches and run in parallel.
    """
    t0 = time.perf_counter()

    client = MongoClient(uri)
    col = client[db_name][raw_col]

    try:
        estimated_docs = col.estimated_document_count()
        log.info("Raw collection estimated documents: %s", f"{estimated_docs:,}")
    except Exception:
        log.info("Raw collection estimated documents: unavailable")

    distinct_t0 = time.perf_counter()
    log.info("Fetching distinct MMSIs...")

    all_mmsis = sorted(col.distinct("mmsi"))

    client.close()

    log.info(
        "Found %d distinct MMSIs in %.1f s",
        len(all_mmsis),
        time.perf_counter() - distinct_t0,
    )

    batches = [
        all_mmsis[i:i + count_batch_size]
        for i in range(0, len(all_mmsis), count_batch_size)
    ]

    total_batches = len(batches)

    log.info(
        "Counting valid pings in parallel: %d MMSIs, %d batches, %d workers",
        len(all_mmsis),
        total_batches,
        num_workers,
    )

    worker_args = [
        (batch_id, batch, min_pings)
        for batch_id, batch in enumerate(batches, start=1)
    ]

    done = 0
    errors = 0
    valid_mmsis = []
    batch_times = []
    log_every = max(1, total_batches // 20)

    with mp.Pool(
        processes=num_workers,
        initializer=_worker_init,
        initargs=(uri, db_name, raw_col, hint_name),
    ) as pool:
        for result in pool.imap_unordered(worker_count_valid_mmsis, worker_args):
            done += 1

            if result["status"] != "ok":
                errors += 1
                log.warning(
                    "Count batch %d failed after %.1f s: %s",
                    result["batch_id"],
                    result["elapsed_s"],
                    result["error"],
                )
            else:
                valid_mmsis.extend(result["mmsis"])
                batch_times.append(result["elapsed_s"])

                log.info(
                    "Count batch %3d/%d | input MMSIs: %d | valid MMSIs: %d | time: %.1f s",
                    result["batch_id"],
                    total_batches,
                    result["input_mmsis"],
                    result["valid_mmsis"],
                    result["elapsed_s"],
                )

            if done % log_every == 0 or done == total_batches:
                elapsed = time.perf_counter() - t0
                pct = done / total_batches * 100
                batches_per_s = done / elapsed if elapsed > 0 else 0

                if batch_times:
                    avg_batch_s = sum(batch_times) / len(batch_times)
                    max_batch_s = max(batch_times)
                else:
                    avg_batch_s = 0.0
                    max_batch_s = 0.0

                log.info(
                    "Count progress: %d/%d batches (%.0f%%) | %.2f batches/s | "
                    "avg %.1f s | max %.1f s | errors %d",
                    done,
                    total_batches,
                    pct,
                    batches_per_s,
                    avg_batch_s,
                    max_batch_s,
                    errors,
                )

    valid_mmsis = sorted(set(valid_mmsis))
    elapsed = time.perf_counter() - t0

    log.info(
        "Found %d vessels with >= %d valid pings in %.1f s (%d errors)",
        len(valid_mmsis),
        min_pings,
        elapsed,
        errors,
    )

    return valid_mmsis


# -- Step 2: reset clean collection ------------------------------------------

def reset_clean_collection(
    uri: str,
    db_name: str,
    clean_col: str,
) -> None:
    client = MongoClient(uri)
    db = client[db_name]

    log.info("Dropping existing %s collection...", clean_col)
    db[clean_col].drop()

    try:
        db.create_collection(clean_col)
        log.info("Created empty %s collection", clean_col)
    except CollectionInvalid:
        log.info("%s already exists", clean_col)

    namespace = f"{db_name}.{clean_col}"

    try:
        log.info("Trying to shard %s by hashed mmsi...", namespace)
        client.admin.command(
            "shardCollection",
            namespace,
            key={"mmsi": "hashed"},
        )
        log.info("%s sharded by hashed mmsi", namespace)

    except OperationFailure as e:
        message = str(e)

        if "already sharded" in message.lower():
            log.info("%s is already sharded", namespace)
        elif "no such command" in message.lower():
            log.info("Sharding skipped. This does not look like mongos.")
        else:
            log.warning("Sharding skipped: %s", e)

    client.close()


# -- Step 3: create indexes after writing -------------------------------------

def create_clean_indexes(uri: str, db_name: str, clean_col: str) -> None:
    client = MongoClient(uri)
    col = client[db_name][clean_col]

    t0 = time.perf_counter()

    log.info("Creating indexes on %s after merge...", clean_col)

    col.create_index(
        [("mmsi", ASCENDING), ("ts", ASCENDING)],
        name="mmsi_ts",
    )
    col.create_index(
        [("ts", ASCENDING)],
        name="ts_only",
    )
    col.create_index(
        [("mmsi", ASCENDING), ("name", ASCENDING)],
        name="mmsi_name",
    )

    elapsed = time.perf_counter() - t0
    client.close()

    log.info("Indexes created in %.1f s", elapsed)


# -- Worker filtering ---------------------------------------------------------

def worker_filter_batch(args: tuple) -> dict:
    batch_id, mmsi_batch, clean_col = args

    t0 = time.perf_counter()

    pipeline = [
        {"$match": {"mmsi": {"$in": mmsi_batch}}},
        {"$match": required_field_conditions()},
        {
            "$merge": {
                "into": clean_col,
                "whenMatched": "keepExisting",
                "whenNotMatched": "insert",
            }
        },
    ]

    try:
        aggregate_raw(pipeline, use_hint=True)

        status = "ok"
        error = None

    except OperationFailure as e:
        status = "error"
        error = str(e)

    elapsed = time.perf_counter() - t0

    return {
        "batch_id": batch_id,
        "vessels": len(mmsi_batch),
        "status": status,
        "error": error,
        "elapsed_s": elapsed,
    }


# -- Main ---------------------------------------------------------------------

def run(
    uri: str,
    db_name: str,
    raw_col: str,
    clean_col: str,
    min_pings: int,
    num_workers: int,
    batch_size: int,
    count_batch_size: int,
    count_index_name: str,
) -> None:
    t0 = time.perf_counter()

    num_workers = max(1, num_workers)
    batch_size = max(1, batch_size)
    count_batch_size = max(1, count_batch_size)

    ensure_raw_indexes(
        uri=uri,
        db_name=db_name,
        raw_col=raw_col,
        count_index_name=count_index_name,
    )

    hint_name = choose_count_hint(
        uri=uri,
        db_name=db_name,
        raw_col=raw_col,
        count_index_name=count_index_name,
    )

    valid_mmsis = find_valid_mmsis_parallel(
        uri=uri,
        db_name=db_name,
        raw_col=raw_col,
        min_pings=min_pings,
        num_workers=num_workers,
        count_batch_size=count_batch_size,
        hint_name=hint_name,
    )

    if not valid_mmsis:
        log.error("No vessels found with >= %d pings. Is data loaded?", min_pings)
        return

    reset_clean_collection(
        uri=uri,
        db_name=db_name,
        clean_col=clean_col,
    )

    batches = [
        valid_mmsis[i:i + batch_size]
        for i in range(0, len(valid_mmsis), batch_size)
    ]

    total_batches = len(batches)

    log.info(
        "Processing %d vessels in %d batches with %d workers",
        len(valid_mmsis),
        total_batches,
        num_workers,
    )

    worker_args = [
        (batch_id, batch, clean_col)
        for batch_id, batch in enumerate(batches, start=1)
    ]

    done = 0
    errors = 0
    batch_times = []
    log_every = max(1, total_batches // 20)

    with mp.Pool(
        processes=num_workers,
        initializer=_worker_init,
        initargs=(uri, db_name, raw_col, hint_name),
    ) as pool:
        for result in pool.imap_unordered(worker_filter_batch, worker_args):
            done += 1

            if result["status"] != "ok":
                errors += 1
                log.warning(
                    "Batch %d failed after %.1f s: %s",
                    result["batch_id"],
                    result["elapsed_s"],
                    result["error"],
                )
            else:
                batch_times.append(result["elapsed_s"])

                log.info(
                    "Batch %3d/%d | vessels: %d | time: %.1f s",
                    result["batch_id"],
                    total_batches,
                    result["vessels"],
                    result["elapsed_s"],
                )

            if done % log_every == 0 or done == total_batches:
                elapsed = time.perf_counter() - t0
                pct = done / total_batches * 100
                batches_per_s = done / elapsed if elapsed > 0 else 0

                if batch_times:
                    avg_batch_s = sum(batch_times) / len(batch_times)
                    max_batch_s = max(batch_times)
                else:
                    avg_batch_s = 0.0
                    max_batch_s = 0.0

                log.info(
                    "Progress: %d/%d batches (%.0f%%) | %.2f batches/s | "
                    "avg batch %.1f s | max batch %.1f s | errors %d",
                    done,
                    total_batches,
                    pct,
                    batches_per_s,
                    avg_batch_s,
                    max_batch_s,
                    errors,
                )

    create_clean_indexes(
        uri=uri,
        db_name=db_name,
        clean_col=clean_col,
    )

    client = MongoClient(uri)
    clean_count = client[db_name][clean_col].estimated_document_count()
    client.close()

    elapsed = time.perf_counter() - t0

    log.info("=" * 70)
    log.info("Filtering complete")
    log.info("Input vessels:        %d", len(valid_mmsis))
    log.info("Batches:              %d", total_batches)
    log.info("Errors:               %d", errors)
    log.info("Clean docs estimate:  %d", clean_count)
    log.info("Time:                 %.1f s", elapsed)

    if clean_count and elapsed > 0:
        log.info("Clean docs/s:          %.0f", clean_count / elapsed)

    log.info("=" * 70)


# -- Entry point --------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Filter AIS vessels and write clean data to MongoDB"
    )

    parser.add_argument("--uri", default=MONGO_URI)
    parser.add_argument("--db", default=DB_NAME)
    parser.add_argument("--raw", default=RAW_COL)
    parser.add_argument("--clean", default=CLEAN_COL)
    parser.add_argument("--min-pings", type=int, default=MIN_PINGS)

    parser.add_argument(
        "--workers",
        type=int,
        default=NUM_WORKERS,
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="MMSIs per filtering batch",
    )

    parser.add_argument(
        "--count-batch-size",
        type=int,
        default=100,
        help="MMSIs per valid-counting batch",
    )

    parser.add_argument(
        "--count-index-name",
        default=DEFAULT_COUNT_INDEX_NAME,
        help="Name of the partial index used for valid-ping counting.",
    )

    args = parser.parse_args()

    run(
        uri=args.uri,
        db_name=args.db,
        raw_col=args.raw,
        clean_col=args.clean,
        min_pings=args.min_pings,
        num_workers=args.workers,
        batch_size=args.batch_size,
        count_batch_size=args.count_batch_size,
        count_index_name=args.count_index_name,
    )
