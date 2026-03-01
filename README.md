# Reverse Geocode

An exploration of offline reverse geocoding — given (lat, lon), return the
country and administrative region in the browser, with no server, no API key,
and no network calls after initial load.

**[Live demo →](https://lulzx.github.io/reverse-geocode/)**

Three separate implementations compare different spatial indexing strategies
at the same problem. A unified web app lets you switch between them at load
time.

    GADM 4.1 (global administrative boundaries, finest available level)
           |
           v
    355 685 regions — ADM2 through ADM5 — covering the entire land surface
           |
        -----------------------------------------------
        |                   |                         |
        v                   v                         v
     z0/                 s2/                        h3/
     RGEO0004            RGEO0001                   LKHA0001
     Zig builder         Python builder             Python builder
     Morton boundary     S2 block binary search     H3 flat binary search
     9.3 MB              27 MB                      13 MB


## The problem

Reverse geocoding is conceptually simple: given a point, find which polygon
contains it. The naive implementation — load all polygons, test each one — is
far too slow for interactive use and the polygon data is hundreds of megabytes.

The challenge is building a data structure that:
- fits in a small binary file (< 60 MB)
- can be loaded entirely into browser memory
- answers queries in microseconds, not milliseconds
- requires no server — pure static file serving

All three implementations solve this by pre-computing the hard part (polygon
containment) at build time and collapsing the result into a compact spatial
index that the browser can search with simple arithmetic.


## Data sources

**GADM 4.1** (Global Administrative Areas) provides boundaries up to ADM5
for many countries — communes in France, villages in Rwanda, suburbs in the
UK and Russia. `extract_gadm.py` queries the 2.6 GB SQLite GeoPackage and
picks the finest available level per feature:

    NAME_5 → ADM5 (communes, villages)   51 427 features
    NAME_4 → ADM4 (townships, suburbs)  147 515 features
    NAME_3 → ADM3 (districts, boroughs) 120 510 features
    NAME_2 → ADM2 (counties, prefects)   36 245 features  (fallback)

Total: **355 697 features** with globally unique GIDs used as dedup keys.

GADM was chosen over alternatives like geoBoundaries because it has global
coverage, consistent structure, and distinguishes country / adm1 / adm2
cleanly at every level.


## The basemap (adm2_render.geojson)

The map rendered in the UI is not tiles. It is a single GeoJSON file
containing all 355 685 administrative boundaries simplified to 0.01° (~1 km)
tolerance, with integer feature IDs matching the geocoder's `admin_id` values.

    geocoder.lookup(lat, lon)
           |
           v  returns admin_id  (integer)
           |
    adminMap.get(admin_id)
           |
           v  returns { country, adm1, adm2 }  from GeoJSON properties

The GeoJSON is separate from the geocoding index because the geocoder only
needs to return an integer — the names live in the basemap which is already
loaded for rendering.

`make_render_geojson.py` builds it from the same prep binary used by the z0
builder, so feature IDs are guaranteed to match.


## Architecture

The unified app (`ui/`) loads two files at startup in parallel:

    fetch("adm2_render.geojson")    fetch("*_geo.bin")
              |                              |
              v                             v
      build adminMap                  parse binary
      stamp country colours           init geocoder
              |                              |
              +------------- both ready -----+
                                   |
                              attach to map

On every mousemove (via requestAnimationFrame):

    mouse position
         |
         v
    map.unproject()  →  (lat, lng)
         |
         +--→  geocoder.lookup(lat, lng)  →  admin_id / null
         |           (timed with performance.now())
         |
         +--→  map.queryRenderedFeatures()  →  feature id for highlight
         |
         v
    map.setFeatureState(id, { on: true })   ← GPU feature-state, zero CPU
    ui.update(names, timing, coords)         ← floating tooltip


## Approach comparison

All three read an in-memory binary blob fetched once at page load and answer
every subsequent query in pure JavaScript.

- **z0** uses a two-layer scheme: a coarse 0.25° grid covers ~97% of land
  queries in a single array lookup; a Morton-sorted boundary table handles
  the rest. Typical lookup: < 2 µs.
- **s2** packs S2 cells into compact 32-bit keys organised into 64-byte
  cache-line blocks for two-level binary search. Consistent ~2 µs regardless
  of location.
- **h3** stores raw 64-bit H3 cell IDs in a flat sorted array and
  binary-searches with a three-resolution fallback (res 6 → 5 → 4).

<!-- prettier table -->
                 z0          s2          h3
    -----------------------------------------------
    index size   9.3 MB      27 MB       13 MB
    interior     ~1.6 µs     ~1.9 µs     ~1.6 µs
    boundary     ~2.4 µs     ~1.9 µs     ~2.7 µs
    ocean        ~0.4 µs     ~2.6 µs     ~3.5 µs
    key type     uint32      uint32      uint64
    key space    Morton      S2 compact  H3 raw
    levels        2           2           1 + fallback
    builder      Zig         Python      Python
    JS BigInt?   no          no          yes

z0 exits early on ocean via the coarse grid (0.4 µs). s2 is the most
consistent land geocoder but pays ~2.6 µs for ocean. h3 uses a fallback
so ocean costs ~3.5 µs.


## z0 binary format — RGEO0004

RGEO0004 replaced RGEO0003's flat Morton-block table with a palette+local-key
grouped stream, cutting the index from 19.7 MB to 9.3 MB (2.1×).

    Header (40 bytes)
      [0:8]   magic "RGEO0004"
      [8:12]  format version (u32)
      [12:16] bitmap offset
      [16:20] rank table offset
      [20:24] values array offset
      [24:28] land cell count
      [28:32] boundary index offset
      [32:36] stream offset
      [36:40] boundary group count

    Sections
      bitmap         129 600 B   1-bit land/ocean flag per 0.25° cell
      rank table       8 100 B   cumulative popcount every 512 bits
      values          ~434 KB   u24 per land cell: admin_id or sentinel
      boundary index  ~717 KB   u32 stream-relative byte offset per group
      stream           ~8.5 MB  variable-length encoded groups (see below)

    Sentinels (u24)
      0xFFFFFF  BOUNDARY — fine group decode required
      0xFFFFFE  OCEAN    — coastal cell with no land polygon

    Morton quantisation: 12-bit (4096 steps per axis, ~2.4 km resolution)

    Group format (one group per boundary coarse cell):
      base_lq   u16 LE   latitude  quantised base (0–4095)
      base_aq   u16 LE   longitude quantised base (0–4095)
      pal_size  u8       palette entry count (1–16)
      rec_count u8       number of fine records in this group
      palette   pal_size × u24 LE   distinct admin_ids in this group
      keys      rec_count × u8      local key = ((lq-base_lq)<<4)|(aq-base_aq)
      indices   ceil(rec_count × idx_bits / 8) B   MSB-first palette indices

    idx_bits: 0 if pal_size≤1, 2 if pal_size≤4, 4 if pal_size≤16


## Data pipeline

```sh
# 1. Extract finest admin level from GADM 4.1 GeoPackage
python extract_gadm.py gadm_410.gpkg gadm_finest.geojson
# → 355 697 features, ADM2–ADM5, with uid = finest GID

# 2. Build z0 geocoder
cd z0
python prepare.py ../gadm_finest.geojson z0_prep_finest.bin
# → 1.1 GB prep file, 355 685 admins, 475 157 polygon parts

zig build -Drelease=true
./zig-out/bin/builder z0_prep_finest.bin z0_geo_finest.bin
# → 19.7 MB RGEO0003 binary in ~32 s

python convert_rgeo4.py z0_geo_finest.bin z0_geo_v4.bin
# → 9.3 MB RGEO0004 binary (2.1× smaller)

python query4.py 48.8566 2.3522 z0_geo_v4.bin
# → (48.8566, 2.3522) -> admin_id 12345

# 3. Build render GeoJSON
python make_render_geojson.py z0/z0_prep_finest.bin ui/public/data/adm2_render.geojson
# → 126 MB simplified GeoJSON, 355 685 features

# 4. Build s2 and h3 (unchanged from ADM2-level data)
cd s2 && python builder.py gadm_adm2.geojson
cd h3 && python builder.py gadm_adm2.geojson && python extract_names.py
```


## Running locally

```sh
# Unified app (all three geocoders, selector on load)
cd ui && bun install && bun dev   # http://localhost:5173

# Standalone apps
cd z0/ui && bun dev   # http://localhost:5174
cd s2/ui && bun dev   # http://localhost:5175
cd h3/ui && bun dev   # http://localhost:5176
```

Data files (`.bin`, `.geojson`) are served from `public/data/` symlinks in dev
and from Cloudflare R2 in production. They are excluded from git (too large).

Dependencies: `bun`, `zig 0.15`, Python 3.10+ with `shapely`, `zstandard`,
`ijson`, `tqdm`.


## UI features

- **Hover tooltip** — flag, country, region, coordinates, lookup time
- **Latency sparkline** — 40×12 px canvas showing last 30 query times
- **Countries counter** — HUD count of unique countries visited this session
- **Click-to-pin** — click to freeze the tooltip; click again to unpin
- **Explore button** — ⟳ flies the map to a random land location
- **Offline indicator** — green dot when geocoder is ready
- **Progress bar** — loading progress for both data files in parallel
