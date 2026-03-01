"""
builder.py — Build h3_geo.bin from a geoBoundaries ADM2 GeoJSON file.

Usage:
    python builder.py <path/to/geoBoundaries-ADM2.geojson>

The input file must be a GeoJSON FeatureCollection whose features have
properties at minimum:
    shapeISO   — ISO 3166-1 alpha-3 country code   (or other country keys)
    shapeName  — district (ADM2) name
    ADM1_NAME  — first-level subdivision name   (many geoBoundaries extracts
                  include this; we also accept "ADM1NAME", "adm1name", etc.)

The builder falls back gracefully when ADM1/ADM2 names are missing.

Output: h3_geo.bin in the current working directory.

h3 v4 note: cell IDs are hex strings; we convert to int immediately for
storage so that the record array contains raw uint64 values and binary
search works correctly.
"""

import json
import multiprocessing
import os
import struct
import sys
from concurrent.futures import ProcessPoolExecutor
import zstandard as zstd
import h3
from shapely.geometry import shape, Polygon, MultiPolygon


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAGIC        = b"LKHA0001"
VERSION      = 1
RES_FINE     = 6   # finer resolution, stored explicitly where needed
RES_COARSE   = 5   # coarser resolution, primary coverage layer
OUTPUT_FILE  = "h3_geo.bin"


# ---------------------------------------------------------------------------
# h3 ID conversion helpers (h3 v4 returns hex strings)
# ---------------------------------------------------------------------------

def cell_to_int(cell: str) -> int:
    """Convert an h3 v4 hex-string cell ID to a uint64 integer."""
    return int(cell, 16)


def int_to_cell(n: int) -> str:
    """Convert a uint64 integer back to an h3 v4 hex-string cell ID."""
    return h3.int_to_str(n)


# ---------------------------------------------------------------------------
# Property extraction helpers
# ---------------------------------------------------------------------------

_COUNTRY_KEYS = ["country", "shapeGroup", "shapeISO", "ISO", "ISO_A3", "GID_0", "ADM0_A3", "PROV_CODE"]
_ADM1_KEYS    = ["adm1", "ADM1_NAME", "ADM1NAME", "adm1name", "NAME_1", "VARNAME_1",
                  "GID_1", "shapeName1", "ADM1"]
_ADM2_KEYS    = ["adm2", "shapeName", "NAME_2", "VARNAME_2", "GID_2", "shapeName2",
                  "ADM2", "district"]


def _first(props, keys, fallback=""):
    for k in keys:
        v = props.get(k)
        if v and str(v).strip():
            return str(v).strip()
    return fallback


def extract_names(props):
    """Return (country_code, adm1_name, adm2_name) from a feature's properties."""
    country = _first(props, _COUNTRY_KEYS, "UNK")
    adm1    = _first(props, _ADM1_KEYS,    "")
    adm2    = _first(props, _ADM2_KEYS,    "")
    return country, adm1, adm2


# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------

def _ensure_polygon_list(geom):
    """Yield simple Shapely Polygon objects from any geometry."""
    if geom is None:
        return
    if isinstance(geom, Polygon):
        if not geom.is_empty and geom.is_valid:
            yield geom
        elif not geom.is_empty:
            # Attempt to fix self-intersections
            fixed = geom.buffer(0)
            if not fixed.is_empty:
                yield from _ensure_polygon_list(fixed)
    elif isinstance(geom, MultiPolygon):
        for part in geom.geoms:
            yield from _ensure_polygon_list(part)
    else:
        # GeometryCollection or other — recurse into sub-geometries
        if hasattr(geom, "geoms"):
            for g in geom.geoms:
                yield from _ensure_polygon_list(g)


def shapely_to_h3poly(polygon: Polygon) -> h3.LatLngPoly:
    """
    Convert a Shapely Polygon to an h3.LatLngPoly.
    h3 expects (lat, lon) coordinate order, i.e. (y, x).
    Shapely stores coordinates as (x=lon, y=lat).
    The closing duplicate vertex of each ring is dropped.
    """
    exterior = [(y, x) for x, y in polygon.exterior.coords[:-1]]
    holes = [
        [(y, x) for x, y in ring.coords[:-1]]
        for ring in polygon.interiors
    ]
    return h3.LatLngPoly(exterior, *holes)


def cells_for_polygon(polygon: Polygon, resolution: int) -> set[int]:
    """
    Return the set of H3 cell IDs (as integers) covering a Shapely Polygon
    at the given resolution.
    """
    h3poly = shapely_to_h3poly(polygon)
    try:
        return {cell_to_int(c) for c in h3.h3shape_to_cells(h3poly, resolution)}
    except Exception:
        # Degenerate or out-of-range polygons — skip silently
        return set()


# ---------------------------------------------------------------------------
# Name table management
# ---------------------------------------------------------------------------

class NameRegistry:
    """
    Manages per-country lookup tables for ADM1 and ADM2 names.

    Bit layout for packed_meta (uint32):
      bits 31-22: country_id      (10 bits, max 1023)
      bits 21-14: state_offset    ( 8 bits, max  255, per-country)
      bits 13-0:  district_offset (14 bits, max 16383, per-country)
    """

    def __init__(self):
        self._country_id: dict[str, int]    = {}
        self._country_names: list[str]      = []
        self._adm1: list[dict[str, int]]    = []   # country_id → {name → idx}
        self._adm1_names: list[list[str]]   = []
        self._adm2: list[dict[str, int]]    = []
        self._adm2_names: list[list[str]]   = []

    def register(self, country_code: str, adm1_name: str, adm2_name: str
                 ) -> tuple[int, int, int]:
        """
        Register the three names and return (country_id, state_offset, district_offset).
        Raises ValueError on overflow.
        """
        cid = self._get_or_add_country(country_code)
        sid = self._get_or_add_adm1(cid, adm1_name)
        did = self._get_or_add_adm2(cid, adm2_name)
        return cid, sid, did

    def _get_or_add_country(self, code: str) -> int:
        if code not in self._country_id:
            cid = len(self._country_names)
            if cid > 1023:
                raise ValueError(f"Country count exceeds 1023: {code!r}")
            self._country_id[code] = cid
            self._country_names.append(code)
            self._adm1.append({})
            self._adm1_names.append([])
            self._adm2.append({})
            self._adm2_names.append([])
        return self._country_id[code]

    def _get_or_add_adm1(self, cid: int, name: str) -> int:
        mapping = self._adm1[cid]
        if name not in mapping:
            idx = len(self._adm1_names[cid])
            if idx > 255:
                raise ValueError(
                    f"ADM1 count for {self._country_names[cid]!r} exceeds 255"
                )
            mapping[name] = idx
            self._adm1_names[cid].append(name)
        return mapping[name]

    def _get_or_add_adm2(self, cid: int, name: str) -> int:
        mapping = self._adm2[cid]
        if name not in mapping:
            idx = len(self._adm2_names[cid])
            if idx > 16383:
                raise ValueError(
                    f"ADM2 count for {self._country_names[cid]!r} exceeds 16383"
                )
            mapping[name] = idx
            self._adm2_names[cid].append(name)
        return mapping[name]

    def to_json_bytes(self) -> bytes:
        obj = {
            "countries": self._country_names,
            "adm1":      self._adm1_names,
            "adm2":      self._adm2_names,
        }
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


# ---------------------------------------------------------------------------
# Packed-meta helpers
# ---------------------------------------------------------------------------

def pack_meta(country_id: int, state_offset: int, district_offset: int) -> int:
    """Encode three identifiers into a single uint32.
    Bits 31-22: country_id (10 bits), bits 21-14: state (8 bits), bits 13-0: district (14 bits).
    """
    return (country_id << 22) | (state_offset << 14) | district_offset


# ---------------------------------------------------------------------------
# Core build function
# ---------------------------------------------------------------------------

def load_features(path: str) -> list:
    """Load features from a GeoJSON file (FeatureCollection or single Feature)."""
    print(f"Loading GeoJSON from {path} …")
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    if data.get("type") == "FeatureCollection":
        features = data.get("features") or []
    elif data.get("type") == "Feature":
        features = [data]
    else:
        features = []
    print(f"  {len(features)} features found")
    return features


def _process_feature_h3(feature):
    """Worker: return (country, adm1, adm2, coarse_cells, fine_cells) for one feature."""
    raw_geom = feature.get("geometry")
    if raw_geom is None:
        return None
    props = feature.get("properties") or {}
    country, adm1, adm2 = extract_names(props)
    geom = shape(raw_geom)
    polygons = list(_ensure_polygon_list(geom))
    if not polygons:
        return None
    coarse = set()
    fine = set()
    for poly in polygons:
        coarse.update(cells_for_polygon(poly, RES_COARSE))
        fine.update(cells_for_polygon(poly, RES_FINE))
    return country, adm1, adm2, coarse, fine


def build(geojson_path: str, workers: int = 0):
    features = load_features(geojson_path)
    registry = NameRegistry()

    # cell_id (int) → packed_meta (int)
    cell_map: dict[int, int] = {}

    total = len(features)
    if workers == 0:
        workers = min(multiprocessing.cpu_count(), 8)
    print(f"  Using {workers} workers")

    try:
        from tqdm import tqdm as _tqdm
        pbar = _tqdm(total=total, desc="  H3 cells", unit="feat")
    except ImportError:
        pbar = None

    chunk = 200
    done = 0
    with ProcessPoolExecutor(max_workers=workers) as executor:
        for start in range(0, total, chunk):
            batch = features[start:start + chunk]
            results = list(executor.map(_process_feature_h3, batch))
            for result in results:
                if result is None:
                    done += 1
                    continue
                country, adm1, adm2, coarse, fine = result
                try:
                    cid, sid, did = registry.register(country, adm1, adm2)
                except ValueError as exc:
                    print(f"\n  WARNING: skipping feature {done}: {exc}")
                    done += 1
                    continue
                meta = pack_meta(cid, sid, did)
                for cell_int in coarse:
                    cell_map[cell_int] = meta
                for cell_int in fine:
                    cell_map[cell_int] = meta
                done += 1
            if pbar:
                pbar.update(len(batch))
            elif done % 1000 == 0 or done == total:
                print(f"  {done}/{total} …", end="\r", flush=True)
    if pbar:
        pbar.close()

    print(f"\n  Total cells (res-5 + res-6): {len(cell_map):,}")

    # -----------------------------------------------------------------------
    # Prune redundant res-6 cells.
    #
    # If a res-6 cell maps to the same admin area as its res-5 parent, the
    # fallback logic in query.py will find the parent entry anyway — no need
    # to store the child entry.  This can reduce the record count by ~40%.
    # -----------------------------------------------------------------------
    print("  Pruning redundant res-6 cells …")
    pruned_map: dict[int, int] = {}
    for cell_int, meta in cell_map.items():
        cell_str = int_to_cell(cell_int)
        if h3.get_resolution(cell_str) == RES_FINE:
            parent_str = h3.cell_to_parent(cell_str, RES_COARSE)
            parent_int = cell_to_int(parent_str)
            if cell_map.get(parent_int) == meta:
                continue  # parent encodes same area; drop child
        pruned_map[cell_int] = meta

    print(f"  Cells after pruning: {len(pruned_map):,}")

    # -----------------------------------------------------------------------
    # Sort by integer cell ID — this is the key used for binary search.
    # -----------------------------------------------------------------------
    sorted_cells = sorted(pruned_map.items())   # list[(cell_int, meta)]
    N = len(sorted_cells)

    # -----------------------------------------------------------------------
    # Compress name table.
    # -----------------------------------------------------------------------
    name_json = registry.to_json_bytes()
    cctx      = zstd.ZstdCompressor(level=19)
    name_zstd = cctx.compress(name_json)
    print(
        f"  Name table: {len(name_json):,} bytes raw "
        f"→ {len(name_zstd):,} bytes zstd"
    )

    # -----------------------------------------------------------------------
    # Write binary file.
    #
    # Layout (all integers little-endian):
    #   [0:8]    magic "LKHA0001"
    #   [8:12]   version uint32
    #   [12:16]  record count N uint32
    #   [16:20]  name-table offset uint32
    #   [20 : 20+N*12]  sorted records: [uint64 h3_index][uint32 packed_meta]
    #   [20+N*12 : ]    zstd-compressed JSON name table
    # -----------------------------------------------------------------------
    name_table_offset = 20 + N * 12

    print(f"  Writing {OUTPUT_FILE} …")
    with open(OUTPUT_FILE, "wb") as fh:
        fh.write(MAGIC)
        fh.write(struct.pack("<I", VERSION))
        fh.write(struct.pack("<I", N))
        fh.write(struct.pack("<I", name_table_offset))

        for cell_int, meta in sorted_cells:
            fh.write(struct.pack("<QI", cell_int, meta))

        fh.write(name_zstd)

    size = os.path.getsize(OUTPUT_FILE)
    print(f"  Done. {OUTPUT_FILE}: {size / 1_048_576:.2f} MB, {N:,} records")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("geojson")
    parser.add_argument("--workers", "-j", type=int, default=0,
                        help="Parallel workers (0=auto)")
    args = parser.parse_args()
    build(args.geojson, args.workers)
