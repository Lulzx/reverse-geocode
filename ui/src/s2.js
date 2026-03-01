/**
 * s2.js — RGEO0001 reverse geocoder, browser runtime.
 *
 * Loads s2_geo.bin and exposes lookup(lat, lon) → admin_id | null.
 * The binary uses H3 cells (res-6 and res-7) encoded as compact uint32 values
 * in a two-level block binary search structure.
 */

import * as h3lib from 'h3-js';

const MAGIC            = 'RGEO0001';
const RECORDS_PER_BLOCK = 10;
const RECORD_SIZE      = 6;    // uint32 cell_id + uint16 admin_id
const BLOCK_SIZE       = 64;   // bytes — one cache line
const HEADER_SIZE      = 64;   // bytes

// enc6 = bits [51:27] of the 64-bit H3 id = hi20<<5 | lo>>27
function _encodeRes6(hi, lo) {
  return ((hi & 0xFFFFF) * 32 + (lo >>> 27)) >>> 0;
}

// enc7 = bits [51:24] of the 64-bit H3 id = hi20<<8 | lo>>24
function _encodeRes7(hi, lo) {
  return ((hi & 0xFFFFF) * 256 + (lo >>> 24)) >>> 0;
}

export class S2 {
  constructor(buffer) {
    const dv    = new DataView(buffer);
    const magic = String.fromCharCode(...new Uint8Array(buffer, 0, 8));
    if (magic !== MAGIC) throw new Error(`s2: bad magic "${magic}"`);

    const l10Count  = dv.getUint32(12, true);
    const l12Count  = dv.getUint32(16, true);
    const l10DirOff = dv.getUint32(20, true);
    const l12DirOff = dv.getUint32(24, true);

    const l10BlockCount = Math.ceil(l10Count / RECORDS_PER_BLOCK);
    const l12BlockCount = Math.ceil(l12Count / RECORDS_PER_BLOCK);

    this._dv             = dv;
    this._l10BlocksOff   = HEADER_SIZE;
    this._l10DirOff      = l10DirOff;
    this._l10BlockCount  = l10BlockCount;
    this._l12BlocksOff   = l10DirOff + l10BlockCount * 4;
    this._l12DirOff      = l12DirOff;
    this._l12BlockCount  = l12BlockCount;
  }

  /** Returns admin_id (uint16) or null for ocean/unclassified. */
  lookup(lat, lon) {
    const cell7Str = h3lib.latLngToCell(lat, lon, 7);
    const p7  = cell7Str.padStart(16, '0');
    const h7  = parseInt(p7.slice(0, 8), 16);
    const l7  = parseInt(p7.slice(8),    16);
    const enc7 = _encodeRes7(h7, l7);

    const cell6Str = h3lib.cellToParent(cell7Str, 6);
    const p6  = cell6Str.padStart(16, '0');
    const h6  = parseInt(p6.slice(0, 8), 16);
    const l6  = parseInt(p6.slice(8),    16);
    const enc6 = _encodeRes6(h6, l6);

    // L10 table (coarse, ~63% of land queries)
    let id = this._blockSearch(enc6, this._l10BlocksOff, this._l10DirOff, this._l10BlockCount);
    if (id !== null) return id;

    // L12 table (fine, ~37% of land queries)
    return this._blockSearch(enc7, this._l12BlocksOff, this._l12DirOff, this._l12BlockCount);
  }

  _blockSearch(cellId, blocksOff, dirOff, blockCount) {
    if (blockCount === 0) return null;
    const dv = this._dv;

    // Binary search the directory for the last block whose first key ≤ cellId
    let lo = 0, hi = blockCount - 1, best = -1;
    while (lo <= hi) {
      const mid      = (lo + hi) >> 1;
      const firstKey = dv.getUint32(dirOff + mid * 4, true);
      if (firstKey <= cellId) { best = mid; lo = mid + 1; }
      else hi = mid - 1;
    }
    if (best < 0) return null;

    // Linear scan within the identified 64-byte block
    const blockStart = blocksOff + best * BLOCK_SIZE;
    for (let i = 0; i < RECORDS_PER_BLOCK; i++) {
      const off     = blockStart + i * RECORD_SIZE;
      const recCell = dv.getUint32(off, true);
      if (recCell === 0 && i > 0) break;    // padding sentinel
      if (recCell === cellId) return dv.getUint16(off + 4, true);
      if (recCell > cellId)   break;        // sorted — past the target
    }
    return null;
  }
}

export async function loadS2(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch ${url}: ${res.status}`);
  return new S2(await res.arrayBuffer());
}
