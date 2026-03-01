"""
query.py — Offline Reverse Geocoder Query Engine

Loads the binary file produced by builder.py and answers reverse-geocode
queries in sub-millisecond time using memory-mapped file access and
block binary search.

S2 / H3 note:
  This implementation uses H3 (pip install h3) as a substitute for the
  S2 geometry library.  Cell IDs are encoded as compact uint32 values
  matching the encoding in builder.py:

    encode_res6(h3_int) = (h3_int >> 27) & 0x1FFFFFF   (25 bits, "level 10")
    encode_res7(h3_int) = (h3_int >> 24) & 0xFFFFFFF   (28 bits, "level 12")

  H3 uses an aperture-7 (not quad-tree) hierarchy, so enc6 != enc7 >> 3
  in general.  The res-6 parent of a res-7 cell is obtained via
  h3.cell_to_parent(cell7_str, 6) and then encoded independently.

Query algorithm (mirrors spec §5):
  1. Compute H3 res-7 cell for (lat, lon)  → enc7   (≈ S2 level 12)
  2. Compute H3 res-6 parent of that cell  → enc6   (≈ S2 level 10)
  3. Block binary search L10 (res-6) table:
       a. Binary search directory for enc6  → block index
       b. Linear scan within 64-byte block → admin_id or miss
  4. If hit → lookup admin table, resolve names → return result  (~63% of queries)
  5. If miss → block binary search L12 (res-7) table for enc7
  6. If hit  → lookup admin table, resolve names → return result  (~37% of queries)
  7. If miss in both → return None (ocean or unclassified territory)

Usage as library:
  from query import ReverseGeocoder
  rg = ReverseGeocoder("s2_geo.bin")
  result = rg.lookup(37.7749, -122.4194)
  # {'country': 'United States', 'adm1': 'California', 'adm2': 'San Francisco'}
  rg.close()

  # Context manager:
  with ReverseGeocoder("s2_geo.bin") as rg:
      result = rg.lookup(48.8566, 2.3522)

Usage as CLI:
  python query.py 37.7749 -122.4194
  python query.py 48.8566 2.3522
  python query.py --data /path/to/s2_geo.bin 35.6762 139.6503
"""

import json
import mmap
import os
import struct
import sys
from typing import Dict, Optional

import h3
import zstandard as zstd

try:
    import numpy as np
    _HAS_NUMPY = True
except ImportError:
    _HAS_NUMPY = False

# ── Constants (must match builder.py exactly) ────────────────────────────────

MAGIC             = b"RGEO0001"
H3_RES_COARSE     = 6
H3_RES_FINE       = 7
RECORDS_PER_BLOCK = 10
RECORD_SIZE       = 6    # uint32 cell_id + uint16 admin_id
BLOCK_SIZE        = 64   # bytes — one cache line
ADMIN_ENTRY_SIZE  = 6    # uint16 country_idx + uint16 adm1_idx + uint16 adm2_idx
HEADER_SIZE       = 64   # bytes


# ── Cell ID encoding (must mirror builder.py exactly) ────────────────────────

def _encode_res6(h3_int: int) -> int:
    """Compact uint32 for an H3 res-6 cell (25 bits)."""
    return (h3_int >> 27) & 0x1FFFFFF


def _encode_res7(h3_int: int) -> int:
    """Compact uint32 for an H3 res-7 cell (28 bits)."""
    return (h3_int >> 24) & 0xFFFFFFF


# ── Core lookup engine ───────────────────────────────────────────────────────

class ReverseGeocoder:
    """
    Memory-mapped reverse geocoder.

    The data file is mmap'd read-only; all internal state is immutable after
    __init__.  Concurrent reads from multiple threads are safe.

    Latency (Python, CPython 3.10+, warm file cache):
      Interior cell (L10 hit):    ~1–2 µs
      Boundary cell (L12 hit):    ~1.5–3 µs
      Ocean / miss (both tables): ~2–4 µs
    """

    def __init__(self, data_path: str = "s2_geo.bin") -> None:
        if not os.path.isfile(data_path):
            raise FileNotFoundError(f"Data file not found: {data_path!r}")

        self._file = open(data_path, "rb")
        self._mm   = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)
        self._parse_header()
        self._load_names()
        if _HAS_NUMPY:
            self._build_numpy_tables()

    def close(self) -> None:
        """Release the mmap and file handle."""
        self._mm.close()
        self._file.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    # ── Header parsing ───────────────────────────────────────────────────────

    def _parse_header(self) -> None:
        mm = self._mm

        magic = mm[0:8]
        if magic != MAGIC:
            raise ValueError(f"Invalid magic bytes: {magic!r}")

        version,    = struct.unpack_from("<I", mm, 8)
        l10_count,  = struct.unpack_from("<I", mm, 12)
        l12_count,  = struct.unpack_from("<I", mm, 16)
        l10_dir_off,= struct.unpack_from("<I", mm, 20)
        l12_dir_off,= struct.unpack_from("<I", mm, 24)
        admin_off,  = struct.unpack_from("<I", mm, 28)
        name_off,   = struct.unpack_from("<I", mm, 32)

        if version != 1:
            raise ValueError(f"Unsupported file version: {version}")

        self._l10_count    = l10_count
        self._l12_count    = l12_count
        self._l10_dir_off  = l10_dir_off
        self._l12_dir_off  = l12_dir_off
        self._admin_off    = admin_off
        self._name_off     = name_off

        # L10 block array begins immediately after the 64-byte header
        self._l10_blocks_off = HEADER_SIZE

        # Block count = ceil(record_count / RECORDS_PER_BLOCK)
        self._l10_block_count = (l10_count + RECORDS_PER_BLOCK - 1) // RECORDS_PER_BLOCK

        # L12 block array begins right after the L10 directory
        self._l12_blocks_off = l10_dir_off + self._l10_block_count * 4

        self._l12_block_count = (l12_count + RECORDS_PER_BLOCK - 1) // RECORDS_PER_BLOCK

    # ── Name table ───────────────────────────────────────────────────────────

    def _load_names(self) -> None:
        mm = self._mm
        compressed = bytes(mm[self._name_off:])
        dctx = zstd.ZstdDecompressor()
        raw  = dctx.decompress(compressed)
        data = json.loads(raw.decode("utf-8"))
        self._countries: list = data["countries"]
        self._adm1s:     list = data["adm1"]
        self._adm2s:     list = data["adm2"]

    # ── Numpy fast-path tables ───────────────────────────────────────────────

    def _build_numpy_tables(self):
        mm = self._mm
        self._l10_cells_np, self._l10_admins_np = self._extract_flat(
            mm, self._l10_blocks_off, self._l10_block_count, self._l10_count)
        self._l12_cells_np, self._l12_admins_np = self._extract_flat(
            mm, self._l12_blocks_off, self._l12_block_count, self._l12_count)

    def _extract_flat(self, mm, blocks_off, block_count, record_count):
        if block_count == 0 or record_count == 0:
            return np.array([], dtype=np.uint32), np.array([], dtype=np.uint16)
        total = block_count * BLOCK_SIZE
        raw = np.frombuffer(mm[blocks_off:blocks_off + total], dtype=np.uint8)
        blocks = raw.reshape(block_count, BLOCK_SIZE)
        rec = np.ascontiguousarray(blocks[:, :RECORDS_PER_BLOCK * RECORD_SIZE].reshape(-1, RECORD_SIZE))
        cells  = np.frombuffer(np.ascontiguousarray(rec[:, :4]).tobytes(), dtype=np.uint32)
        admins = np.frombuffer(np.ascontiguousarray(rec[:, 4:]).tobytes(), dtype=np.uint16)
        return cells[:record_count].copy(), admins[:record_count].copy()

    # ── Admin table ──────────────────────────────────────────────────────────

    def _lookup_admin(self, admin_id: int) -> Dict[str, str]:
        """Resolve a uint16 admin_id to a {'country', 'adm1', 'adm2'} dict."""
        off = self._admin_off + admin_id * ADMIN_ENTRY_SIZE
        c_idx, a1_idx, a2_idx = struct.unpack_from("<HHH", self._mm, off)
        return {
            "country": self._countries[c_idx],
            "adm1":    self._adm1s[a1_idx],
            "adm2":    self._adm2s[a2_idx],
        }

    # ── Block binary search ──────────────────────────────────────────────────

    def _block_search(
        self,
        cell_id: int,
        blocks_off: int,
        dir_off: int,
        block_count: int,
    ) -> Optional[int]:
        if _HAS_NUMPY and hasattr(self, '_l10_cells_np'):
            # Select which table based on blocks_off
            if blocks_off == self._l10_blocks_off:
                cells, admins = self._l10_cells_np, self._l10_admins_np
            else:
                cells, admins = self._l12_cells_np, self._l12_admins_np
            # Must pass np.uint32 — passing a Python int to a uint32 array
            # triggers Python-level fallback comparisons (250x slower).
            uid = np.uint32(cell_id)
            pos = int(np.searchsorted(cells, uid))
            if pos < len(cells) and cells[pos] == uid:
                return int(admins[pos])
            return None

        # original fallback ...
        if block_count == 0:
            return None
        mm = self._mm
        lo, hi = 0, block_count - 1
        best = -1
        while lo <= hi:
            mid = (lo + hi) >> 1
            first_key, = struct.unpack_from("<I", mm, dir_off + mid * 4)
            if first_key <= cell_id:
                best = mid
                lo = mid + 1
            else:
                hi = mid - 1
        if best < 0:
            return None
        block_start = blocks_off + best * BLOCK_SIZE
        for i in range(RECORDS_PER_BLOCK):
            rec_off = block_start + i * RECORD_SIZE
            rec_cell, = struct.unpack_from("<I", mm, rec_off)
            if rec_cell == 0 and i > 0:
                break
            if rec_cell == cell_id:
                admin_id, = struct.unpack_from("<H", mm, rec_off + 4)
                return admin_id
            if rec_cell > cell_id:
                break
        return None

    # ── Public API ───────────────────────────────────────────────────────────

    def lookup(self, lat: float, lon: float) -> Optional[Dict[str, str]]:
        """
        Reverse-geocode a coordinate.

        Args:
          lat: latitude  in decimal degrees  [-90, +90]
          lon: longitude in decimal degrees  [-180, +180]

        Returns:
          dict with keys 'country', 'adm1', 'adm2' for land areas,
          or None for ocean / unclassified territory.
        """
        # Step 1: compute fine (res-7) cell and encode it
        cell7_str = h3.latlng_to_cell(lat, lon, H3_RES_FINE)
        enc7      = _encode_res7(h3.str_to_int(cell7_str))

        # Step 2: compute coarse (res-6) parent of the fine cell and encode it
        #         (H3 aperture-7 hierarchy: must use cell_to_parent, not bitshift)
        cell6_str = h3.cell_to_parent(cell7_str, H3_RES_COARSE)
        enc6      = _encode_res6(h3.str_to_int(cell6_str))

        # Step 3: L10 lookup (interior cells — ~63% of land queries)
        admin_id = self._block_search(
            enc6,
            self._l10_blocks_off,
            self._l10_dir_off,
            self._l10_block_count,
        )
        if admin_id is not None:
            return self._lookup_admin(admin_id)

        # Step 4: L12 lookup (boundary cells — ~37% of land queries)
        admin_id = self._block_search(
            enc7,
            self._l12_blocks_off,
            self._l12_dir_off,
            self._l12_block_count,
        )
        if admin_id is not None:
            return self._lookup_admin(admin_id)

        # Step 5: not found — ocean or unclassified territory
        return None


# ── CLI entry point ──────────────────────────────────────────────────────────

def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Offline reverse geocoder — look up a coordinate in s2_geo.bin.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("lat",  type=float, help="Latitude in decimal degrees.")
    parser.add_argument("lon",  type=float, help="Longitude in decimal degrees.")
    parser.add_argument(
        "--data", "-d",
        default="s2_geo.bin",
        help="Path to the binary data file.",
    )
    args = parser.parse_args()

    with ReverseGeocoder(args.data) as rg:
        result = rg.lookup(args.lat, args.lon)

    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
