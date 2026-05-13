# AIS Vessel Noise Filtering, Assignment 3

Parallel data pipeline using a MongoDB sharded cluster to load, filter and analyse AIS vessel tracking data from the Danish Maritime Authority.

**Dataset:** `aisdk-2026-04-18.csv` - http://aisdata.ais.dk/aisdk-2026-04-18.zip

---

## Project structure

```
config.py               Shared constants
docker-compose.yml      MongoDB sharded cluster definition
load_to_mongo.py        Parallel CSV loader
filter_vessels.py       Parallel noise filtering
delta_t_histogram.py    Delta-t computation and histograms
README.md               Project documentation
```

---

## Prerequisites

```bash
pip install pymongo pandas matplotlib numpy
```

Docker Desktop must be running. If a local MongoDB service is already using port `27017`, stop it first.

```bash
# Windows
net stop MongoDB
```

---

## Configuration

Common settings are stored in `config.py`.

| Constant | Default | Purpose |
|---|---:|---|
| `MONGO_URI` | `mongodb://localhost:27017` | MongoDB query router address |
| `DB_NAME` | `ais` | Database name |
| `RAW_COL` | `vessels_raw` | Raw AIS collection |
| `CLEAN_COL` | `vessels_clean` | Filtered AIS collection |
| `NUM_WORKERS` | `min(8, os.cpu_count())` | Default worker count |
| `MIN_PINGS` | `100` | Minimum valid pings per vessel |
| `MMSI_MIN` | `200000000` | Minimum accepted MMSI |
| `MMSI_MAX` | `999999999` | Maximum accepted MMSI |

The config also stores:

- AIS CSV column names.
- Required input columns.
- Optional columns to read.
- Invalid placeholder MMSI values.
- Null-like strings.
- Allowed mobile types.

---

## MongoDB sharded cluster

### Architecture

```
mongos         (query router,    port 27017)
configsvr      (shard metadata,  port 27019)
shard1         (data shard 1,    port 27018)
shard2         (data shard 2,    port 27020)
mongo-init     (setup container)
```

The init container does this:

1. Starts the config server replica set.
2. Starts shard 1 replica set.
3. Starts shard 2 replica set.
4. Adds both shards to the cluster.
5. Enables sharding on the `ais` database.
6. Shards `ais.vessels_raw` by hashed `mmsi`.

`vessels_clean` is not prepared in Docker Compose. It is recreated and sharded by `filter_vessels.py` each time filtering runs.

### Start the cluster

```bash
docker compose up -d
docker compose logs -f mongo-init
```
Wait until `mongo-init` finishes.

### Verify the cluster

Open the Mongo shell:

```bash
mongosh "mongodb://localhost:27017/?directConnection=false"
```

Run:

```javascript
use admin
sh.status()
```

Both shards should appear in the output.

### Stop / reset

Stop containers and keep data:

```bash
docker compose down
```

Stop containers and delete all data volumes:

```bash
docker compose down -v
```

Use `down -v` for a full clean reset.

---

## Parallel data loading

```bash
python load_to_mongo.py aisdk-2026-04-18.csv
```

### How it works

`load_to_mongo.py` reads a large AIS CSV file and inserts valid rows into MongoDB collection `ais.vessels_raw`.

The script:

1. Reads the CSV header.
2. Verifies required columns.
3. Reads only needed columns.
4. Reads data in chunks of `400000` rows.
5. Splits each chunk into `num_workers * 2` parts.
6. Sends parts to a persistent multiprocessing pool.
7. Each worker cleans its own part.
8. Each worker inserts with its own `MongoClient`.

### Filters applied during loading

 A row is inserted only if:

- `Type of mobile` equals `Class A`.
- MMSI parses as a number.
- MMSI is within `200000000` to `999999999`.
- MMSI is not one of the known invalid placeholder values.
- Timestamp parses from `DD/MM/YYYY HH:MM:SS`.
- Latitude is within `-90` to `90`.
- Longitude is within `-180` to `180`.
- Coordinates are not exactly `0.0, 0.0`.

### Duplicate prevention

Each document gets a deterministic `_id`:

```text
mmsi_timestamp_lat_lon
```

Duplicate AIS pings are rejected by MongoDB as duplicate key errors.

### Arguments

| Argument | Default | Description |
|---|---:|---|
| `csv_file` | required | AIS CSV path |
| `--uri` | `config.MONGO_URI` | MongoDB URI |
| `--db` | `config.DB_NAME` | Database name |
| `--collection` | `config.RAW_COL` | Target collection |
| `--batch-size` | `50000` | Documents per `insert_many` call |
| `--workers` | `config.NUM_WORKERS` | Worker process count |

---

## Parallel noise filtering

```bash
python filter_vessels.py
```

### How it works

`filter_vessels.py` filters raw AIS data and writes clean records to `ais.vessels_clean`.

The script runs in two parallel phases.

**Phase 1 - find valid MMSIs**

The script:

1. Creates or confirms raw collection indexes.
2. Fetches distinct MMSIs.
3. Splits MMSIs into counting batches.
4. Counts valid pings per MMSI batch in parallel.
5. Keeps only MMSIs with at least `MIN_PINGS` valid pings.

A valid ping must have these field types:

```text
mmsi       number
lat        number
lon        number
sog        number
cog        number
heading    number
rot        number
nav_status string
```

**Phase 2 - filter and write**

Each worker receives a batch of valid MMSIs and runs this pipeline:

The script:

1. Drops old `vessels_clean`.
2. Creates empty `vessels_clean`.
3. Tries to shard it by hashed `mmsi`.
4. Splits valid MMSIs into filtering batches.
5. Runs one aggregation pipeline per batch.
6. Writes matching documents with `$merge`.
7. Creates clean collection indexes after writing.

The worker aggregation has this structure:

```text
$match MMSI batch
$match required field type checks
$merge into vessels_clean
```

The documents stay inside MongoDB. Python only coordinates batches.

### Indexes created on `vessels_raw`

The filtering script creates or confirms these indexes by default.

Partial index for valid pings:

```javascript
db.vessels_raw.createIndex(
  { mmsi: 1 },
  {
    name: "valid_required_mmsi_partial",
    partialFilterExpression: {
      mmsi:       { $type: "number" },
      lat:        { $type: "number" },
      lon:        { $type: "number" },
      sog:        { $type: "number" },
      cog:        { $type: "number" },
      heading:    { $type: "number" },
      rot:        { $type: "number" },
      nav_status: { $type: "string" }
    }
  }
)
```

Compound index for vessel-time queries:

```javascript
db.vessels_raw.createIndex(
  { mmsi: 1, ts: 1 },
  { name: "mmsi_ts" }
)
```

Time index:

```javascript
db.vessels_raw.createIndex(
  { ts: 1 },
  { name: "ts_only" }
)
```

Indexes are created after data loading, not before. This avoids slowing down the import.

### Indexes created on `vessels_clean`

Indexes are created after `$merge` finishes:

```javascript
db.vessels_clean.createIndex(
  { mmsi: 1, ts: 1 },
  { name: "mmsi_ts" }
)

db.vessels_clean.createIndex(
  { ts: 1 },
  { name: "ts_only" }
)

db.vessels_clean.createIndex(
  { mmsi: 1, name: 1 },
  { name: "mmsi_name" }
)
```

### Arguments

| Argument | Default | Description |
|---|---:|---|
| `--uri` | `config.MONGO_URI` | MongoDB URI |
| `--db` | `config.DB_NAME` | Database name |
| `--raw` | `config.RAW_COL` | Source collection |
| `--clean` | `config.CLEAN_COL` | Output collection |
| `--min-pings` | `config.MIN_PINGS` | Minimum valid pings per vessel |
| `--workers` | `config.NUM_WORKERS` | Worker process count |
| `--batch-size` | `100` | MMSIs per filtering batch |
| `--count-batch-size` | `100` | MMSIs per valid-counting batch |
| `--count-index-name` | `valid_required_mmsi_partial` | Partial index name |

---

## Task 4 - Delta-t calculation and histograms

```bash
python delta_t_histogram.py
```

### How it works

`delta_t_histogram.py` analyses the time gaps between consecutive AIS pings.

The script:

1. Fetches distinct MMSIs from `vessels_clean`.
2. Splits MMSIs into batches.
3. Processes batches in parallel.
4. Each worker owns its own `MongoClient`.
5. MongoDB computes previous timestamps with `$setWindowFields`.
6. MongoDB computes intervals with `$dateDiff`.
7. Python receives only numeric `dt_ms` values.
8. Python saves histogram images.

### Output

If the default output is used:

```text
delta_t_histogram_full.png
delta_t_histogram_zoomed.png
```

| File | Meaning |
|---|---|
| `delta_t_histogram_full.png` | Full interval distribution |
| `delta_t_histogram_zoomed.png` | Zoomed 0 to 60 second distribution |

### Results

| Metric | Value |
|---|---|
| Ping pairs analysed | 7,550,816 |
| Median interval | 10,000 ms (10.0 s) |
| Mean interval | 17,012 ms (17.0 s) |
| 95th percentile | 30,000 ms (30.0 s) |
| Max interval | 74,985 s (~21 hours) |
| Min interval | 1,000 ms |

### Analysis

The dominant spike at 10 seconds matches the IMO Class A requirement for vessels moving at 0–14 knots. The mean (17,0 s) exceeds the median (10 s) because occasional long gaps pull the average up - these are candidates for Going Dark anomaly detection. The maximum gap of ~21 hours represents a vessel that disabled its transponder for an extended period.

### Arguments

| Argument | Default | Description |
|---|---:|---|
| `--uri` | `config.MONGO_URI` | MongoDB URI |
| `--db` | `config.DB_NAME` | Database name |
| `--collection` | `config.CLEAN_COL` | Source collection |
| `--workers` | `config.NUM_WORKERS` | Worker process count |
| `--batch-size` | `50` | MMSIs per worker batch |
| `--output` | `delta_t_histogram.png` | Output filename base |

## Full workflow

Run the pipeline in this order.

```bash
docker compose up -d
docker compose logs -f mongo-init

python load_to_mongo.py aisdk-2026-04-18.csv

python filter_vessels.py

python delta_t_histogram.py
```

## Presentation of the Solution

Video: https://youtu.be/x3gNXv3Xm8c

