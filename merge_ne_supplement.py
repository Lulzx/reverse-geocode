"""
merge_ne_supplement.py — Merge adm2_render.geojson with Natural Earth ADM1 for missing countries.

Reads:
  z0/ui/public/data/adm2_render.geojson  (GADM ADM2, 180 countries)
  ne_admin1_10m.json                      (Natural Earth 10m ADM1, 251 countries)

Writes:
  z0/ui/public/data/adm2_render_supplemented.geojson

The supplement adds NE ADM1 features (with normalized property keys) for any
country not already represented in the GADM dataset.  This fills in missing
small island nations (SGP, GRL, MUS, CPV, MDV, SYC, CYP, BHR, etc.) so that
s2/h3 geocoders have worldwide coverage.

Property normalization for NE ADM1 features:
  adm0_a3  → country   (ISO 3166-1 alpha-3)
  name     → adm1      (state/province name)
  ""       → adm2      (no ADM2 for NE data, leave empty)
"""

import json
import sys

GADM_PATH = "z0/ui/public/data/adm2_render.geojson"
NE_PATH   = "ne_admin1_10m.json"
OUT_PATH  = "z0/ui/public/data/adm2_render_supplemented.geojson"


def main():
    print(f"Loading {GADM_PATH} …")
    with open(GADM_PATH, encoding="utf-8") as f:
        gadm = json.load(f)
    gadm_features = gadm.get("features", [])
    print(f"  {len(gadm_features)} GADM features")

    # Collect countries already present in GADM
    gadm_countries = set()
    for feat in gadm_features:
        c = feat.get("properties", {}).get("country", "")
        if c:
            gadm_countries.add(c)
    print(f"  {len(gadm_countries)} unique countries in GADM")

    print(f"Loading {NE_PATH} …")
    with open(NE_PATH, encoding="utf-8") as f:
        ne = json.load(f)
    ne_features = ne.get("features", [])
    print(f"  {len(ne_features)} NE ADM1 features")

    # Filter NE features to countries missing from GADM; normalize properties
    supplement = []
    ne_countries_added = set()
    for feat in ne_features:
        props = feat.get("properties", {})
        country = props.get("adm0_a3", "").strip()
        if not country or country in gadm_countries:
            continue
        adm1 = (props.get("name") or props.get("name_local") or "").strip()
        new_feat = {
            "type": "Feature",
            "geometry": feat.get("geometry"),
            "properties": {
                "country": country,
                "adm1": adm1,
                "adm2": "",
            },
        }
        supplement.append(new_feat)
        ne_countries_added.add(country)

    print(f"  Adding {len(supplement)} NE features for {len(ne_countries_added)} missing countries")
    print(f"  Missing countries filled: {sorted(ne_countries_added)}")

    combined = gadm_features + supplement
    print(f"  Total features: {len(combined)}")

    out = {
        "type": "FeatureCollection",
        "features": combined,
    }
    print(f"Writing {OUT_PATH} …")
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
    import os
    size_mb = os.path.getsize(OUT_PATH) / 1_048_576
    print(f"  Done. {OUT_PATH}: {size_mb:.1f} MB, {len(combined)} features")


if __name__ == "__main__":
    main()
