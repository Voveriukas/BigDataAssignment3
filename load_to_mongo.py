"""
Usage:
    python load_to_mongo.py aisdk-2026-04-18.csv
"""

import argparse
import logging
import math
import multiprocessing as mp
import os
import sys
import time

import pandas as pd
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

from config import (
    MONGO_URI, DB_NAME, RAW_COL, NUM_WORKERS,
    ALLOWED_MOBILE_TYPES, INVALID_MMSI_EXACT, MMSI_MIN, MMSI_MAX,
    NULL_STRINGS, REQUIRED_INPUT_COLUMNS, USECOLS,
    COL_MOBILE_TYPE, COL_MMSI, COL_TIMESTAMP, COL_LAT, COL_LON,
    COL_NAV_STATUS, COL_ROT, COL_SOG, COL_COG, COL_HEADING,
    COL_SHIP_TYPE, COL_NAME, COL_DRAUGHT, COL_DEST, COL_IMO,
)
 
CHUNK_SIZE = 400_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# -- Worker MongoDB connection ------------------------------------------------

_worker_client = None
_worker_col = None


def _worker_init(uri: str, db_name: str, col_name: str) -> None:
    """
    Called once per worker process.

    Each worker keeps its own MongoClient instance.
    """
    global _worker_client, _worker_col

    _worker_client = MongoClient(
        uri,
        serverSelectionTimeoutMS=10000,
        connectTimeoutMS=10000,
        socketTimeoutMS=None,
        maxPoolSize=1,
    )
    _worker_col = _worker_client[db_name][col_name]


# -- Helpers ------------------------------------------------------------------

def get_existing_usecols(filepath: str) -> list[str]:
    header = pd.read_csv(filepath, nrows=0).columns.tolist()

    missing_required = [
        col for col in REQUIRED_INPUT_COLUMNS
        if col not in header
    ]

    if missing_required:
        raise ValueError(
            "Missing required CSV columns: "
            + ", ".join(missing_required)
        )

    existing = [col for col in USECOLS if col in header]

    missing_optional = [
        col for col in USECOLS
        if col not in existing
    ]

    if missing_optional:
        log.warning(
            "Missing optional CSV columns: %s",
            ", ".join(missing_optional),
        )

    return existing


def split_dataframe(df: pd.DataFrame, parts: int) -> list[pd.DataFrame]:
    if df.empty:
        return []

    parts = max(1, min(parts, len(df)))
    step = math.ceil(len(df) / parts)

    return [
        df.iloc[start:start + step].copy()
        for start in range(0, len(df), step)
    ]


def to_float_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([None] * len(df), index=df.index, dtype="object")

    return pd.to_numeric(df[col], errors="coerce")


def to_text_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([None] * len(df), index=df.index, dtype="object")

    s = df[col].astype("string").str.strip()
    lower = s.str.lower()

    bad = s.isna() | lower.isin(NULL_STRINGS)
    s = s.mask(bad, None)

    return s.astype("object")


def maybe_float(value):
    if value is None or pd.isna(value):
        return None
    return float(value)


def maybe_int(value):
    if value is None or pd.isna(value):
        return None
    return int(float(value))


def maybe_str(value):
    if value is None or pd.isna(value):
        return None

    text = str(value).strip()
    if not text or text.lower() in NULL_STRINGS:
        return None

    return text


def make_id(mmsi: int, ts, lat: float, lon: float) -> str:
    return f"{mmsi}_{int(ts.timestamp())}_{lat:.6f}_{lon:.6f}"


# -- Chunk processing ---------------------------------------------------------

def process_chunk(chunk: pd.DataFrame) -> list[dict]:
    """
    Cleans one CSV chunk part and converts it to MongoDB documents.

    This runs inside each worker process.
    """
    if chunk.empty:
        return []

    mobile_type = chunk[COL_MOBILE_TYPE].astype("string").str.strip()
    chunk = chunk[mobile_type.isin(ALLOWED_MOBILE_TYPES)]

    if chunk.empty:
        return []

    chunk = chunk.copy()

    chunk["mmsi"] = pd.to_numeric(chunk[COL_MMSI], errors="coerce")
    chunk["lat"] = pd.to_numeric(chunk[COL_LAT], errors="coerce")
    chunk["lon"] = pd.to_numeric(chunk[COL_LON], errors="coerce")
    chunk["ts"] = pd.to_datetime(
        chunk[COL_TIMESTAMP],
        format="%d/%m/%Y %H:%M:%S",
        errors="coerce",
        utc=True,
    )

    valid = (
        chunk["mmsi"].between(MMSI_MIN, MMSI_MAX)
        & ~chunk["mmsi"].isin(INVALID_MMSI_EXACT)
        & chunk["lat"].between(-90.0, 90.0)
        & chunk["lon"].between(-180.0, 180.0)
        & ~((chunk["lat"] == 0.0) & (chunk["lon"] == 0.0))
        & chunk["ts"].notna()
    )

    chunk = chunk[valid]

    if chunk.empty:
        return []

    out = pd.DataFrame(index=chunk.index)

    out["mmsi"] = chunk["mmsi"].astype("int64")
    out["ts"] = chunk["ts"]
    out["lat"] = chunk["lat"].astype("float64")
    out["lon"] = chunk["lon"].astype("float64")

    out["sog"] = to_float_series(chunk, COL_SOG)
    out["cog"] = to_float_series(chunk, COL_COG)
    out["heading"] = to_float_series(chunk, COL_HEADING)
    out["rot"] = to_float_series(chunk, COL_ROT)
    out["draught"] = to_float_series(chunk, COL_DRAUGHT)

    out["nav_status"] = to_text_series(chunk, COL_NAV_STATUS)
    out["ship_type"] = to_text_series(chunk, COL_SHIP_TYPE)
    out["name"] = to_text_series(chunk, COL_NAME)
    out["destination"] = to_text_series(chunk, COL_DEST)
    out["imo"] = to_text_series(chunk, COL_IMO)


    out = out.drop_duplicates(
        subset=["mmsi", "ts", "lat", "lon"],
        keep="first",
    )
    out = out.astype("object").where(pd.notna(out), None)

    docs = []

    for row in out.itertuples(index=False):
        ts = row.ts.to_pydatetime() if hasattr(row.ts, "to_pydatetime") else row.ts
        mmsi = int(row.mmsi)
        lat = float(row.lat)
        lon = float(row.lon)

        docs.append({
            "_id": make_id(mmsi, ts, lat, lon),
            "mmsi": mmsi,
            "ts": ts,
            "lat": lat,
            "lon": lon,
            "sog": maybe_float(row.sog),
            "cog": maybe_float(row.cog),
            "heading": maybe_int(row.heading),
            "rot": maybe_float(row.rot),
            "nav_status": maybe_str(row.nav_status),
            "ship_type": maybe_str(row.ship_type),
            "name": maybe_str(row.name),
            "destination": maybe_str(row.destination),
            "imo": maybe_str(row.imo),
            "draught": maybe_float(row.draught),
        })

    return docs


# -- Worker -------------------------------------------------------------------

def worker_process_and_insert(args: tuple) -> dict:
    """
    Worker task.

    Each worker:
      1. cleans its DataFrame part,
      2. builds MongoDB documents,
      3. inserts documents with its own MongoClient.
    """
    chunk_part, batch_size, worker_id = args

    t0 = time.perf_counter()
    docs = process_chunk(chunk_part)
    process_s = time.perf_counter() - t0

    inserted = 0
    write_errors = 0
    insert_s = 0.0
    duplicate_errors = 0
    other_errors = 0

    if docs and _worker_col is not None:
        t1 = time.perf_counter()

        for start in range(0, len(docs), batch_size):
            batch = docs[start:start + batch_size]

            try:
                result = _worker_col.insert_many(
                    batch,
                    ordered=False,
                    bypass_document_validation=True,
                )
                inserted += len(result.inserted_ids)

            except BulkWriteError as e:
                inserted += e.details.get("nInserted", 0)

                errors = e.details.get("writeErrors", [])

                batch_duplicate_errors = sum(
                    1 for err in errors
                    if err.get("code") == 11000
                )

                batch_other_errors = len(errors) - batch_duplicate_errors

                duplicate_errors += batch_duplicate_errors
                other_errors += batch_other_errors
                write_errors += len(errors)

        insert_s = time.perf_counter() - t1

    return {
        "worker_id": worker_id,
        "rows": len(chunk_part),
        "valid": len(docs),
        "inserted": inserted,
        "skipped": len(chunk_part) - len(docs),
        "write_errors": write_errors,
        "duplicate_errors": duplicate_errors,
        "other_errors": other_errors,
        "process_s": process_s,
        "insert_s": insert_s,
    }


# -- Loader -------------------------------------------------------------------

def load_csv(
    filepath: str,
    uri: str,
    db_name: str,
    col_name: str,
    batch_size: int,
    num_workers: int,
) -> None:
    if not os.path.exists(filepath):
        log.error("File not found: %s", filepath)
        sys.exit(1)

    batch_size = max(1, batch_size)
    num_workers = max(1, num_workers)

    try:
        usecols = get_existing_usecols(filepath)
    except ValueError as e:
        log.error(str(e))
        sys.exit(1)

    file_size_mb = os.path.getsize(filepath) / 1_048_576

    log.info("Loading: %s (%.0f MB)", filepath, file_size_mb)
    log.info("Target: %s db=%s col=%s", uri, db_name, col_name)
    log.info("Workers: %d | Batch size: %d docs", num_workers, batch_size)
    log.info("CSV columns read: %d", len(usecols))

    t0 = time.perf_counter()

    total_rows = 0
    total_valid = 0
    total_inserted = 0
    total_skipped = 0
    chunk_num = 0
    total_duplicate_errors = 0
    total_other_errors = 0
    total_write_errors = 0

    with mp.Pool(
        processes=num_workers,
        initializer=_worker_init,
        initargs=(uri, db_name, col_name),
    ) as pool:

        for chunk in pd.read_csv(
            filepath,
            chunksize=CHUNK_SIZE,
            dtype=str,
            usecols=usecols,
            low_memory=False,
            on_bad_lines="skip",
        ):
            chunk_num += 1
            total_rows += len(chunk)

            chunk_t0 = time.perf_counter()

            parts = split_dataframe(chunk, parts=num_workers * 2)

            worker_args = [
                (part, batch_size, i % num_workers)
                for i, part in enumerate(parts)
            ]

            results = list(
                pool.imap_unordered(
                    worker_process_and_insert,
                    worker_args,
                )
            )

            chunk_wall_s = time.perf_counter() - chunk_t0

            chunk_valid = sum(r["valid"] for r in results)
            chunk_inserted = sum(r["inserted"] for r in results)
            chunk_skipped = sum(r["skipped"] for r in results)
            chunk_duplicate_errors = sum(r["duplicate_errors"] for r in results)
            chunk_other_errors = sum(r["other_errors"] for r in results)
            chunk_write_errors = sum(r["write_errors"] for r in results)
            
            max_process_s = max((r["process_s"] for r in results), default=0.0)
            max_insert_s = max((r["insert_s"] for r in results), default=0.0)

            total_valid += chunk_valid
            total_inserted += chunk_inserted
            total_skipped += chunk_skipped
            total_duplicate_errors += chunk_duplicate_errors
            total_other_errors += chunk_other_errors
            total_write_errors += chunk_write_errors
            elapsed = time.perf_counter() - t0
            rows_per_sec = total_rows / elapsed if elapsed > 0 else 0
            inserted_per_sec = total_inserted / elapsed if elapsed > 0 else 0

            log.info(
                "Chunk %3d | rows: %d | valid: %d | inserted: %d | "
                "skipped: %d | write errors: %d | duplicates: %d | other errors: %d",
                chunk_num,
                len(chunk),
                chunk_valid,
                chunk_inserted,
                chunk_skipped,
                chunk_write_errors,
                chunk_duplicate_errors,
                chunk_other_errors,
            )

            log.info(
                "Chunk %3d | wall %.2fs | max process %.2fs | "
                "max insert %.2fs | %.0f rows/s | %.0f inserted/s",
                chunk_num,
                chunk_wall_s,
                max_process_s,
                max_insert_s,
                rows_per_sec,
                inserted_per_sec,
            )

    elapsed = time.perf_counter() - t0

    log.info("=" * 70)
    log.info("Done")
    log.info("Rows read:      %d", total_rows)
    log.info("Valid docs:     %d", total_valid)
    log.info("Inserted docs:  %d", total_inserted)
    log.info("Skipped rows:   %d", total_skipped)
    log.info("Write errors:   %d", total_write_errors)
    log.info("Duplicate errors: %d", total_duplicate_errors)
    log.info("Other errors:   %d", total_other_errors)
    log.info("Time:           %.1f s", elapsed)
    log.info("Rows/s:         %.0f", total_rows / elapsed if elapsed > 0 else 0)
    log.info("Inserted/s:     %.0f", total_inserted / elapsed if elapsed > 0 else 0)
    log.info("=" * 70)


# -- Entry point --------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load AIS CSV into MongoDB in parallel"
    )

    parser.add_argument("csv_file")
    parser.add_argument("--uri", default=MONGO_URI)
    parser.add_argument("--db", default=DB_NAME)
    parser.add_argument("--collection", default=RAW_COL)
    parser.add_argument("--batch-size", type=int, default=50000)
    parser.add_argument(
        "--workers",
        type=int,
        default=NUM_WORKERS,
    )

    args = parser.parse_args()

    load_csv(
        filepath=args.csv_file,
        uri=args.uri,
        db_name=args.db,
        col_name=args.collection,
        batch_size=args.batch_size,
        num_workers=args.workers,
    )
