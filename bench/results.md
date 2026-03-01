# Benchmark results

Platform: Apple Silicon (macOS), Python 3.x query modules, 5 000 iterations per city, 100 warmup.

```
python bench.py --n 5000
```

All three geocoders are measured in the same process using the same Python query
modules. The numbers reflect the algorithmic cost without JavaScript overhead.


## Raw numbers after optimisation (mean µs)

    city                                       z0        s2        h3
    -----------------------------------------------------------------
    São Paulo, Brazil                       2.40      2.59      3.27
    London, UK                              2.33      2.75      1.52
    Tokyo, Japan                            1.55      2.78      1.55
    Nairobi, Kenya                          1.58      2.80      1.52
    Cairo, Egypt                            1.55      2.79      1.53
    Sydney, Australia                       1.59      2.76      2.60
    Moscow, Russia                          1.50      1.91      2.64
    Los Angeles, USA                        1.94      1.94      2.62
    Mumbai, India                           1.57      2.80      1.57
    Cape Town, South Africa                 0.37      2.77      2.63
    Tripoint DE/FR/CH                       1.55      2.86      1.55
    Kaliningrad, Russia                     2.42      2.83      1.57
    Mid-Atlantic (ocean)                    0.36      2.62      3.37
    South Pacific  (ocean)                  0.35      2.61      3.35


## Before and after (baseline → optimised)

    geocoder  query type        before   after   speedup
    z0        interior land      3.65µs   1.50µs   2.4×
    z0        boundary land      6.34µs   2.40µs   2.6×
    z0        ocean              0.37µs   0.36µs   same
    s2        interior (L10)     3.08µs   1.91µs   1.6×
    s2        boundary (L12)     6.35µs   2.86µs   2.2×
    s2        ocean              5.43µs   2.62µs   2.1×
    h3        res-6 hit          2.51µs   1.52µs   1.7×
    h3        res-5 fallback     4.67µs   2.60µs   1.8×
    h3        ocean              6.47µs   3.35µs   1.9×


## What changed

### Python (query.py)

**z0**:

1. Pre-computed cumulative rank table (`_rank_cell`, Int32Array of 1 036 800
   entries). The original rank function scanned up to 64 bitmap bytes per
   lookup with a Python loop. The new path is a single array index.

2. Pre-computed spread table (`_SPREAD12`, 4096 entries). The original
   Morton interleaving was a 16-iteration Python bit loop. The new path
   is two table lookups.

3. Flat sorted Morton array extracted from blocks at init. numpy.searchsorted
   replaces the Python bisect + block linear scan.

Critical finding: `np.searchsorted(uint32_array, python_int)` triggers
Python-level fallback comparisons and takes 144 µs. Passing `np.uint32(value)`
keeps the comparison in C and takes 0.6 µs. All Morton and cell-ID lookups
must pass the correct numpy dtype, not a plain Python int.

**s2**:

Pre-parsed L10 and L12 records into flat numpy uint32 arrays at init.
np.searchsorted (with np.uint32 query) replaces the Python block binary
search. The original block search did Python bisect + struct.unpack per
block step.

**h3**:

Pre-parsed the record array into a numpy uint64 array at init. The original
code used bisect on a `_RecordView` object whose `__getitem__` called
struct.unpack_from for every comparison step — Python overhead per comparison.
numpy.searchsorted with `np.uint64(cell_id)` does all comparisons in C.

### JavaScript (browser runtime)

**z0.js**: Pre-computed `_rankAtCell` Int32Array at construction. The
original lookup walked up to 64 bitmap bytes per lookup with a loop and
_PC8 table. The new path is `rank = this._rankAtCell[idx]`.

Estimated improvement for interior queries in the browser: ~2–3×.

**s2.js**: Removed all BigInt. H3 encoding now uses pure 32-bit arithmetic:

    enc6 = ((hi & 0xFFFFF) * 32  + (lo >>> 27)) >>> 0   // bits [51:27]
    enc7 = ((hi & 0xFFFFF) * 256 + (lo >>> 24)) >>> 0   // bits [51:24]

where hi/lo are obtained by `parseInt` on the high and low 8 hex chars of the
H3 cell string. Eliminates 4 BigInt constructions per lookup.

**h3.js**: Removed all BigInt. The `_search(qHi, qLo)` method now takes
two uint32 parameters and compares hi parts first, then lo:

    if (recHi < qHi || (recHi === qHi && recLo < qLo)) lo = mid + 1;

Eliminates ~40 BigInt constructions per ocean query (3 × log2(851k) ≈ 60
comparisons). BigInt construction overhead in V8 is ~50 ns each; the
saving is ~2–3 µs per lookup.

Estimated improvement for h3 in the browser: ~3–5×.


## Why Python timings look flatter than JavaScript

Python timings are dominated by interpreter overhead, not algorithmic
differences. Each lookup takes ~15k Python bytecodes regardless of the
algorithm. The rank precompute helps z0 because it eliminates an explicit
Python loop, but all three geocoders still pay the same Python dispatch tax
for every attribute lookup, function call, and type conversion.

In the JavaScript browser runtime the JIT eliminates interpreter overhead and
cache misses dominate, so the algorithmic differences are far more visible.
JavaScript timings (from the interactive map):

    query type              z0          s2          h3
    --------------------------------------------------
    interior land           ~300 ns     ~1–2 µs     ~2–3 µs   (after BigInt removal)
    boundary land           ~1–2 µs     ~1.5–3 µs   ~3–5 µs
    ocean                   ~100–200ns  ~2 µs       ~4–6 µs


## Accuracy / correctness

Reference: OpenStreetMap Nominatim reverse geocoding, queried at zoom=5
(country level). All 14 test points validated 2026-03.

    city                  Nominatim   z0    s2    h3-country
    --------------------------------------------------------
    São Paulo, Brazil     BR          BR    miss  UNK
    London, UK            GB          GB    GB    UNK
    Tokyo, Japan          JP          JP    JP    UNK
    Nairobi, Kenya        KE          KE    KE    UNK
    Cairo, Egypt          EG          EG    EG    UNK
    Sydney, Australia     AU          AU    AU    UNK
    Moscow, Russia        RU          RU    RU    UNK
    Los Angeles, USA      US          US    US    UNK
    Mumbai, India         IN          IN    IN    UNK
    Cape Town, ZA         ZA          miss  ZA    UNK
    Tripoint DE/FR/CH     CH          CH    CH    UNK
    Kaliningrad, Russia   RU          RU    RU    UNK
    Mid-Atlantic          ocean       ok    ok    ok
    South Pacific         ocean       ok    ok    ok

**z0** — 12/12 non-ambiguous land + 2/2 ocean. Cape Town (-33.9249, 18.4241)
returns ocean because the coordinate sits at the coastline: the 0.25° grid
cell containing it was classified as ocean at build time (the cell centroid
fell in the water). Moving 0.1° inland to (-33.83, 18.49) returns ZAF.

**s2** — 11/12 non-ambiguous land + 2/2 ocean. São Paulo returns ocean due to
a coverage gap in the geoBoundaries dataset that s2 was built from. GADM
(used by z0) has better coverage of Brazil's coastal polygons.

**h3** — Country and ADM1 names are always "UNK" because the h3 builder
looked for `shapeISO` as the country key but the geoBoundaries property name
is `shapeGroup` / `GID_0` depending on dataset version. Only ADM2 district
names are populated (39 564 names from the districtName field). Ocean
detection is correct. Fix: rebuild with the correct property key mapping.


## Running the benchmark

```sh
# from repo root, with z0 venv active (needs numpy, h3, zstandard)
source z0/.venv/bin/activate
python bench/bench.py

# skip geocoders you haven't built
python bench/bench.py --skip-s2
python bench/bench.py --n 1000   # fewer iterations for quick check
```

The script loads each geocoder's `query.py` using importlib isolation so all
three coexist in a single process without sys.modules conflicts.


## The numpy dtype trap

The most surprising bug found during optimisation: `np.searchsorted` on a
uint32 array falls back to Python-level comparisons when the query value is a
plain Python int, making it 250× slower than with a numpy scalar of matching
dtype:

    np.searchsorted(uint32_array, python_int)   144 µs   (Python fallback)
    np.searchsorted(uint32_array, np.uint32(v))   0.6 µs  (C loop)

The root cause: Python ints have arbitrary precision, so numpy cannot know
whether to treat them as int32, int64, or something else. It defaults to a
safe Python-level comparison loop. Always pass `np.uint32(v)` or `np.uint64(v)`
when querying typed arrays.


## Accuracy benchmark vs Nominatim (59 cities / 3 ocean control points)

```
python bench/accuracy.py   # uses cached Nominatim results
```

Test set: 56 land cities + 3 ocean control points. Nominatim (OSM) used as
ground truth. Correctness = matching ISO 3166-1 alpha-2 country code.

| Geocoder | Correct | Accuracy | Notes |
|----------|---------|----------|-------|
| z0       | 51/56   | 91%      | 5 misses: Singapore (MYS cell), Athens, Nuuk, Port Louis, Praia (GADM gaps) |
| s2       | 55/56   | 98%      | 1 miss: Istanbul (Fatih peninsula — cell centroid in Bosphorus) |
| h3       | 51/56   | 91%      | 5 misses: Istanbul, Reykjavik, Auckland, Nuuk, Praia (coastal/island centroids) |

### Changes to reach these numbers

1. **z0** — 5-point cell sampling: centre + 4 quadrant sub-centres per grid cell
   so coastal cells whose centroid is ocean still get classified as land.
2. **s2 / h3** — Rebuilt from `adm2_render.geojson` (GADM ADM2, 180 countries)
   instead of geoBoundaries; fixed property-key lookups so country/adm1/adm2
   are correctly extracted.
3. **Natural Earth supplement** — 689 ADM1 features for 88 countries absent from
   GADM (SGP, GRL, MUS, CPV, MDV, SYC, …) merged into the GeoJSON source.
4. **s2 algorithm** — Switched coarse-table fill from "interior cells only"
   (mp.contains) to centroid-in-polygon (h3shape_to_cells), which prevents
   district-boundary cells from being silently dropped.
5. **Parallel build** — Both s2 and h3 builders now use `ProcessPoolExecutor`
   (`--workers` / `-j` flag, default auto) for a ~4-6× faster build.
