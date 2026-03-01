# Reverse Geocode

An exploration of offline reverse geocoding — given (lat, lon), return the
country and administrative region in the browser, with no server, no API key,
and no network calls after initial load.

Three separate implementations were built to compare different spatial indexing
strategies at the same problem. Each has its own binary format, builder, and
interactive map UI.

    GADM 4.1 (global administrative boundaries, ADM2 level)
           |
           v
    47 205 regions covering the entire land surface
           |
        -----------------------------------------------
        |                   |                         |
        v                   v                         v
     z0/                 s2/                        h3/
     RGEO0002            RGEO0001                   LKHA0001
     Zig builder         Python builder             Python builder
     Morton boundary     H3 block binary search     H3 flat binary search
     11 MB               55 MB                      10 MB
     port 5174           port 5175                  port 5176


## The problem

Reverse geocoding is conceptually simple: given a point, find which polygon
contains it. The naive implementation — load all polygons, test each one — is
far too slow for interactive use and the polygon data is hundreds of megabytes.

The challenge is building a data structure that:
- fits in a small binary file (< 60 MB, ideally < 15 MB)
- can be loaded entirely into browser memory
- answers queries in microseconds, not milliseconds
- requires no server — pure static file serving

All three implementations solve this by pre-computing the hard part (polygon
containment) at build time and collapsing the result into a compact spatial
index that the browser can search with simple arithmetic.


## Data sources

**GADM 4.1** (Global Administrative Areas) provides ADM2-level boundaries for
the entire world: districts, counties, prefectures — the second level of
administrative subdivision within each country.

The data comes as a SQLite database. `extract_gadm.py` queries it to produce
a flat GeoJSON FeatureCollection with `country`, `adm1`, and `adm2` properties
on each feature.

GADM was chosen over alternatives like geoBoundaries because:
- it has global coverage with no significant gaps
- the polygons are consistent in structure
- it distinguishes country / adm1 / adm2 cleanly

**Natural Earth** (10m ADM1) supplements GADM for the 88 countries absent from
it — small island nations and territories such as Singapore, Greenland,
Mauritius, Cape Verde, Maldives, Seychelles, and many overseas territories.
`merge_ne_supplement.py` merges the two datasets into a single GeoJSON
(`adm2_render_supplemented.geojson`, 47 894 features) which the s2 and h3
builders use as their source. Without this supplement those countries would
return no result for any coordinate.


## The basemap (adm2_render.geojson)

The map rendered in each UI is not tiles. It is a single GeoJSON file
containing all 47 205 administrative boundaries, simplified to 0.01° (~1 km)
tolerance, with integer feature IDs that match the geocoder's `admin_id`
values.

    geocoder.lookup(lat, lon)
           |
           v returns admin_id  (integer 0–47204)
           |
    adminMap.get(admin_id)
           |
           v returns { country, adm1, adm2 }  from GeoJSON properties


The GeoJSON is separate from the geocoding index because the geocoder only
needs to return an integer — the names live in the basemap which is already
loaded for rendering. This avoids duplicating name data in the binary.

`make_render_geojson.py` builds it from the same prep binary used by the z0
builder, so the feature IDs are guaranteed to match.


## Architecture shared by all three UIs

Each UI loads two files at startup in parallel:

    fetch("adm2_render.geojson")    fetch("*_geo.bin")
              |                              |
              v                             v
      build adminMap                  parse binary
      stamp country colours           init geocoder
              |                              |
              +------------- both ready -----+
                                   |
                              attach to map
                         (layers: land, dividers, hl, hl-border)


On every mousemove (via requestAnimationFrame to avoid redundant frames):

    mouse position
         |
         v
    map.unproject()  →  (lat, lng)
         |
         +--→  geocoder.lookup(lat, lng)  →  admin_id / {names} / null
         |           (timed with performance.now())
         |
         +--→  map.queryRenderedFeatures()  →  feature id for highlight
         |
         v
    map.setFeatureState(id, { on: true })   ← GPU feature-state, zero CPU
    ui.update(names, timing, coords)         ← floating tooltip


The highlight is decoupled from the geocoder result so it always reflects the
rendered map, even if the geocoder and GeoJSON were built from different data.


## Approach comparison

All three read an in-memory binary blob fetched once at page load and answer
every subsequent query in pure JavaScript with no network calls.

- **z0** uses a two-layer scheme: a coarse 0.25° grid covers ~97% of land
  queries in a single array lookup, and a Morton-sorted boundary table handles
  the rest. Lookup is typically < 1 µs.
- **s2** packs H3 cells into compact 32-bit keys and organises them into
  64-byte cache-line blocks for two-level binary search. Interior queries hit
  the coarse (res-6) table; boundary queries fall through to the fine (res-7)
  table.
- **h3** stores raw 64-bit H3 cell IDs in a flat sorted array and binary-searches
  with a three-resolution fallback (res 6 → 5 → 4). The 64-bit keys require
  BigInt arithmetic in JavaScript but the implementation is the simplest of the
  three.

<!-- prettier table -->
                 z0          s2          h3
    -----------------------------------------------
    index size   11 MB       33 MB       10 MB
    interior     ~1.6 µs     ~1.9 µs     ~1.6 µs
    boundary     ~2.4 µs     ~1.9 µs     ~2.7 µs
    ocean        ~0.4 µs     ~2.6 µs     ~3.5 µs
    key type     uint32      uint32      uint64
    key space    Morton      H3 compact  H3 raw
    levels        2           2           1 + fallback
    builder      Zig         Python      Python
    JS BigInt?   no          no          yes

Measured with `bench/bench.py` (5 000 iterations, 100 warmup, Apple Silicon):

    city                               z0        s2        h3
    ---------------------------------------------------------
    São Paulo, Brazil               2.42µs    1.95µs    2.65µs
    London, UK                      2.39µs    1.92µs    2.67µs
    Tokyo, Japan                    1.53µs    2.82µs    2.68µs
    Nairobi, Kenya                  1.59µs    1.92µs    1.60µs
    Cairo, Egypt                    1.61µs    1.91µs    1.61µs
    Sydney, Australia               1.54µs    1.93µs    1.62µs
    Moscow, Russia                  1.65µs    1.95µs    1.63µs
    Los Angeles, USA                2.41µs    1.85µs    2.77µs
    Mumbai, India                   1.62µs    1.90µs    1.64µs
    Cape Town, South Africa         1.64µs    1.90µs    2.77µs
    Tripoint DE/FR/CH               1.61µs    1.90µs    1.62µs
    Kaliningrad, Russia             2.43µs    1.92µs    2.77µs
    Mid-Atlantic (ocean)            0.38µs    2.59µs    3.43µs
    South Pacific (ocean)           0.39µs    2.62µs    3.50µs

z0 exits early on ocean via the coarse grid (0.4 µs). s2 is the most
consistent land geocoder (1.85–1.95 µs regardless of location) but pays
~2.6 µs for ocean. h3 uses a three-resolution fallback so ocean costs
~3.5 µs.

See `bench/` for full methodology.


## Accuracy

Benchmarked against Nominatim (OpenStreetMap) ground truth on 56 major world
cities + 3 ocean control points:

<!-- prettier table -->
                 z0          s2          h3
    -----------------------------------------------
    correct      51 / 56     55 / 56     51 / 56
    accuracy     91 %        98 %        91 %

**s2** achieves 98% — only Istanbul is missed (the benchmark coordinate lands
in an H3 cell whose centroid falls in the Bosphorus strait).

**z0** and **h3** miss 5 cities each.  z0 misses Singapore, Athens, Nuuk,
Port Louis, and Praia — the 0.25° grid cell centroid falls in open water for
each (narrow coastlines / small islands).  h3 shares the same root cause:
Istanbul, Reykjavik, Auckland, Nuuk, and Praia all have their lookup cell
centroid in water.

All remaining misses are inherent to centroid-based spatial indexing on
narrow peninsulas and small islands, not data gaps.

Run `python bench/accuracy.py` to reproduce (uses cached Nominatim results).


## Running

```sh
cd z0/ui && bun dev   # http://localhost:5174
cd s2/ui && bun dev   # http://localhost:5175
cd h3/ui && bun dev   # http://localhost:5176
```

Dependencies: `bun`, `zig` (for z0 builder), Python 3.10+ with `shapely`,
`zstandard`, `h3`, `ijson`.


## Data pipeline

```sh
# 1. Extract from GADM SQLite
python extract_gadm.py gadm_4.1.gpkg gadm_adm2.geojson

# 2. Build each geocoder
cd z0 && python prepare.py ../gadm_adm2.geojson z0_prep.bin
        zig build -Drelease=true
        ./zig-out/bin/builder z0_prep.bin z0_geo.bin

cd s2 && python builder.py ../gadm_adm2.geojson
cd h3 && python builder.py ../gadm_adm2.geojson
        python extract_names.py   # pre-extract names for browser

# 3. Build the render GeoJSON
python make_render_geojson.py z0/z0_prep.bin z0/ui/public/data/adm2_render.geojson
```
