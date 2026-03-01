"""
query.py — Load h3_geo.bin and serve reverse-geocode lookups.

Usage (library):
    from query import ReverseGeocoder
    gc = ReverseGeocoder("h3_geo.bin")
    result = gc.lookup(37.7749, -122.4194)
    # → {"country": "USA", "state": "California", "district": "San Francisco"}
    # → None  (if ocean / unmapped)

Usage (CLI):
    python query.py <lat> <lon> [path/to/h3_geo.bin]
"""

import bisect
import json
import mmap
import os
import struct
import sys
from typing import Optional

import zstandard as zstd
import h3

try:
    import numpy as np
    _HAS_NUMPY = True
except ImportError:
    _HAS_NUMPY = False


# ---------------------------------------------------------------------------
# File-format constants  (must match builder.py exactly)
# ---------------------------------------------------------------------------

MAGIC   = b"LKHA0001"
VERSION = 1

# Header layout
HDR_SIZE           = 20          # bytes before the record array
RECORD_SIZE        = 12          # uint64 + uint32
H3_INDEX_SIZE      = 8           # bytes for the cell-ID key within each record
META_SIZE          = 4           # bytes for packed_meta

# Resolution ladder for the fallback search
RES_FINE     = 6
RES_COARSE   = 5
RES_COARSEST = 4


# ---------------------------------------------------------------------------
# Bit-unpacking helpers
# ---------------------------------------------------------------------------

def unpack_meta(packed: int) -> tuple[int, int, int]:
    """Decompose a packed uint32 into (country_id, state_offset, district_offset).
    Bits 31-22: country_id (10 bits), bits 21-14: state (8 bits), bits 13-0: district (14 bits).
    """
    country_id      = (packed >> 22) & 0x3FF
    state_offset    = (packed >> 14) & 0xFF
    district_offset =  packed        & 0x3FFF
    return country_id, state_offset, district_offset


# ---------------------------------------------------------------------------
# Binary search helpers
#
# The record array is a flat byte string (or mmap slice) of N × 12-byte
# records sorted by the first 8 bytes (uint64 LE h3_index).
#
# bisect operates on a *view object* that translates integer indices to
# 8-byte LE uint64 comparisons without materialising the whole array.
# ---------------------------------------------------------------------------

class _RecordView:
    """
    Adapts a bytes-like object of packed records so that bisect can treat it
    as a sorted sequence of uint64 keys (the H3 cell IDs).
    """

    __slots__ = ("_buf", "_n")

    def __init__(self, buf, n: int):
        self._buf = buf
        self._n   = n

    def __len__(self) -> int:
        return self._n

    def __getitem__(self, i: int) -> int:
        offset = i * RECORD_SIZE
        # Read only the 8-byte key; avoid unpacking the trailing meta uint32
        return struct.unpack_from("<Q", self._buf, offset)[0]


# ---------------------------------------------------------------------------
# ReverseGeocoder
# ---------------------------------------------------------------------------

class ReverseGeocoder:
    """
    Memory-mapped reverse geocoder backed by h3_geo.bin.

    The file is kept open for the lifetime of this object.  Lookups are
    thread-safe as long as no two threads call `close()` concurrently.
    """

    def __init__(self, bin_path: str = "h3_geo.bin"):
        self._bin_path = bin_path
        self._fh       = None
        self._mm       = None
        self._records  = None   # mmap slice of the record array (bytes-like)
        self._n        = 0      # number of records
        self._view     = None   # _RecordView wrapping self._records
        self._names    = None   # decoded name table dict

        self._load(bin_path)

    # -- Initialisation ------------------------------------------------------

    def _load(self, path: str):
        self._fh = open(path, "rb")
        self._mm = mmap.mmap(self._fh.fileno(), 0, access=mmap.ACCESS_READ)

        # --- validate magic -------------------------------------------------
        magic = self._mm[:8]
        if magic != MAGIC:
            raise ValueError(f"Bad magic: {magic!r}; expected {MAGIC!r}")

        # --- parse header ---------------------------------------------------
        version, n_records, name_table_off = struct.unpack_from(
            "<III", self._mm, 8
        )
        if version != VERSION:
            raise ValueError(f"Unsupported version: {version}")

        self._n = n_records

        # Slice the mmap for the record array so bisect works directly on it.
        records_start = HDR_SIZE
        records_end   = HDR_SIZE + n_records * RECORD_SIZE
        self._records = self._mm[records_start:records_end]

        self._view = _RecordView(self._records, n_records)

        if _HAS_NUMPY:
            self._build_numpy_tables()

        # --- load and decompress name table ---------------------------------
        name_blob = self._mm[name_table_off:]
        dctx = zstd.ZstdDecompressor()
        name_json = dctx.decompress(bytes(name_blob))
        self._names = json.loads(name_json)

    # -- Numpy fast-path tables ----------------------------------------------

    def _build_numpy_tables(self):
        n = self._n
        # Parse 12-byte records: [uint64 h3_id][uint32 meta]
        # Use custom dtype with explicit itemsize to avoid alignment padding
        raw = np.frombuffer(self._records, dtype=np.uint8).reshape(n, RECORD_SIZE)
        id_bytes   = np.ascontiguousarray(raw[:, :H3_INDEX_SIZE])
        meta_bytes = np.ascontiguousarray(raw[:, H3_INDEX_SIZE:])
        self._h3_ids_np = np.frombuffer(id_bytes.tobytes(),   dtype=np.uint64)
        self._metas_np  = np.frombuffer(meta_bytes.tobytes(), dtype=np.uint32)

    def _search_np(self, cell_id):
        uid = np.uint64(cell_id)
        pos = int(np.searchsorted(self._h3_ids_np, uid))
        if pos < self._n and self._h3_ids_np[pos] == uid:
            return int(self._metas_np[pos])
        return None

    # -- Core lookup ---------------------------------------------------------

    def lookup(self, lat: float, lon: float) -> Optional[dict]:
        """
        Reverse-geocode a (lat, lon) coordinate.

        Returns a dict with keys "country", "state", "district", or None if
        the point falls in ocean / unmapped territory.

        h3 v4 returns cell IDs as hex strings; we convert to int immediately
        because the binary file stores raw uint64 integers.
        """
        # Convert (lat, lon) to H3 cell string at finest resolution, then int
        cell_str = h3.latlng_to_cell(lat, lon, RES_FINE)
        cell_int = int(cell_str, 16)

        # Try res 6 → res 5 → res 4 (each level is one cell_to_parent call)
        for res in (RES_FINE, RES_COARSE, RES_COARSEST):
            if res != RES_FINE:
                cell_str = h3.cell_to_parent(cell_str, res)
                cell_int = int(cell_str, 16)

            meta = self._search_np(cell_int) if (_HAS_NUMPY and hasattr(self, '_h3_ids_np')) else self._search(cell_int)
            if meta is not None:
                return self._decode_meta(meta)

        return None  # ocean or genuinely unmapped

    # -- Binary search -------------------------------------------------------

    def _search(self, cell_id: int) -> Optional[int]:
        """
        Binary search the sorted record array for cell_id.
        Returns the packed uint32 meta on hit, None on miss.
        """
        pos = bisect.bisect_left(self._view, cell_id)
        if pos < self._n and self._view[pos] == cell_id:
            # Read the 4-byte meta that follows the 8-byte key
            offset = pos * RECORD_SIZE + H3_INDEX_SIZE
            (packed,) = struct.unpack_from("<I", self._records, offset)
            return packed
        return None

    # -- Name resolution -----------------------------------------------------

    def _decode_meta(self, packed: int) -> dict:
        cid, sid, did = unpack_meta(packed)

        countries = self._names["countries"]
        adm1_list = self._names["adm1"]
        adm2_list = self._names["adm2"]

        country  = countries[cid]  if cid < len(countries)         else ""
        state    = adm1_list[cid][sid] if (
            cid < len(adm1_list) and sid < len(adm1_list[cid])
        ) else ""
        district = adm2_list[cid][did] if (
            cid < len(adm2_list) and did < len(adm2_list[cid])
        ) else ""

        return {"country": country, "state": state, "district": district}

    # -- Resource management -------------------------------------------------

    def close(self):
        """Release the mmap and file handle."""
        if self._mm is not None:
            self._mm.close()
            self._mm = None
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    # -- Convenience ---------------------------------------------------------

    @property
    def record_count(self) -> int:
        return self._n


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) < 2:
        print("Usage: python query.py <lat> <lon> [path/to/h3_geo.bin]")
        sys.exit(1)

    lat = float(args[0])
    lon = float(args[1])
    bin_path = args[2] if len(args) >= 3 else "h3_geo.bin"

    if not os.path.exists(bin_path):
        print(f"Error: {bin_path!r} not found. Build it first with builder.py.")
        sys.exit(1)

    with ReverseGeocoder(bin_path) as gc:
        result = gc.lookup(lat, lon)

    if result is None:
        print("None (ocean or unmapped)")
    else:
        print(f"country:  {result['country']}")
        print(f"state:    {result['state']}")
        print(f"district: {result['district']}")
