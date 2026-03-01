/**
 * h3.js — LKHA0001 reverse geocoder, browser runtime.
 *
 * Loads h3_geo.bin + h3_names.json and exposes lookup(lat, lon).
 * The binary stores raw uint64 H3 cell IDs (res 4/5/6) in a flat sorted
 * array; each record is 12 bytes: uint64 cell_id + uint32 packed_meta.
 */

import * as h3lib from 'h3-js';

const MAGIC      = 'LKHA0001';
const HDR_SIZE   = 20;
const RECORD_SIZE = 12;  // uint64 h3_index + uint32 packed_meta
const RES_FINE     = 6;
const RES_COARSE   = 5;
const RES_COARSEST = 4;

export class H3 {
  constructor(buffer, names) {
    const dv    = new DataView(buffer);
    const magic = String.fromCharCode(...new Uint8Array(buffer, 0, 8));
    if (magic !== MAGIC) throw new Error(`h3: bad magic "${magic}"`);

    this._dv       = dv;
    this._nRecords = dv.getUint32(12, true);
    this._names    = names;
  }

  /** Returns {country, adm1, adm2} or null for ocean/unclassified. */
  lookup(lat, lon) {
    let cellStr = h3lib.latLngToCell(lat, lon, RES_FINE);

    for (const res of [RES_FINE, RES_COARSE, RES_COARSEST]) {
      if (res !== RES_FINE) cellStr = h3lib.cellToParent(cellStr, res);
      const padded = cellStr.padStart(16, '0');
      const qHi   = parseInt(padded.slice(0, 8), 16);
      const qLo   = parseInt(padded.slice(8),    16);
      const packed = this._search(qHi, qLo);
      if (packed !== null) return this._decode(packed);
    }
    return null;
  }

  _search(qHi, qLo) {
    const dv = this._dv;
    let lo = 0, hi = this._nRecords - 1;
    while (lo <= hi) {
      const mid   = (lo + hi) >> 1;
      const off   = HDR_SIZE + mid * RECORD_SIZE;
      const recLo = dv.getUint32(off,     true);
      const recHi = dv.getUint32(off + 4, true);
      if      (recHi < qHi || (recHi === qHi && recLo < qLo)) lo = mid + 1;
      else if (recHi > qHi || (recHi === qHi && recLo > qLo)) hi = mid - 1;
      else    return dv.getUint32(off + 8, true);
    }
    return null;
  }

  _decode(packed) {
    // Bit layout: bits 31-22 = country_id (10 bits), 21-14 = state (8 bits), 13-0 = district (14 bits)
    const cid = (packed >>> 22) & 0x3FF;
    const sid = (packed >>> 14) & 0xFF;
    const did =  packed         & 0x3FFF;
    const n   = this._names;
    return {
      country: n.countries[cid]       ?? '',
      adm1:    n.adm1[cid]?.[sid]    ?? '',
      adm2:    n.adm2[cid]?.[did]    ?? '',
    };
  }
}

export async function loadH3(binUrl, namesUrl) {
  const [binRes, namesRes] = await Promise.all([fetch(binUrl), fetch(namesUrl)]);
  if (!binRes.ok)   throw new Error(`Failed to fetch ${binUrl}: ${binRes.status}`);
  if (!namesRes.ok) throw new Error(`Failed to fetch ${namesUrl}: ${namesRes.status}`);
  const [buf, names] = await Promise.all([binRes.arrayBuffer(), namesRes.json()]);
  return new H3(buf, names);
}
