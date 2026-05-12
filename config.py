import os

# -- MongoDB ------------------------------------------------------------------

MONGO_URI  = "mongodb://localhost:27017"
DB_NAME    = "ais"
RAW_COL    = "vessels_raw"
CLEAN_COL  = "vessels_clean"

# -- Parallelism --------------------------------------------------------------

NUM_WORKERS = min(8, os.cpu_count() or 1)

# -- Filtering thresholds -----------------------------------------------------

MIN_PINGS = 100

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

# -- AIS CSV column names -----------------------------------------------------

COL_MOBILE_TYPE = "Type of mobile"
COL_MMSI        = "MMSI"
COL_TIMESTAMP   = "# Timestamp"
COL_LAT         = "Latitude"
COL_LON         = "Longitude"
COL_NAV_STATUS  = "Navigational status"
COL_ROT         = "ROT"
COL_SOG         = "SOG"
COL_COG         = "COG"
COL_HEADING     = "Heading"
COL_SHIP_TYPE   = "Ship type"
COL_NAME        = "Name"
COL_DRAUGHT     = "Draught"
COL_DEST        = "Destination"
COL_IMO         = "IMO"

# -- Validation ---------------------------------------------------------------

ALLOWED_MOBILE_TYPES = {"Class A"}

INVALID_MMSI_EXACT = {0, 111111111, 123456789, 222222222, 999999999}
MMSI_MIN = 200_000_000
MMSI_MAX = 999_999_999

NULL_STRINGS = {"", "nan", "none", "unknown", "undefined"}

# -- CSV columns to read (skip others to reduce memory) -----------------------

REQUIRED_INPUT_COLUMNS = [
    COL_MOBILE_TYPE,
    COL_MMSI,
    COL_TIMESTAMP,
    COL_LAT,
    COL_LON,
]

USECOLS = [
    COL_MOBILE_TYPE,
    COL_MMSI,
    COL_TIMESTAMP,
    COL_LAT,
    COL_LON,
    COL_NAV_STATUS,
    COL_ROT,
    COL_SOG,
    COL_COG,
    COL_HEADING,
    COL_SHIP_TYPE,
    COL_NAME,
    COL_DRAUGHT,
    COL_DEST,
    COL_IMO,
]