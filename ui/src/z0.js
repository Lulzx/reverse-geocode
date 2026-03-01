/**
 * z0.js — Offline reverse geocoder, browser runtime.
 *
 * Loads z0_geo.bin (RGEO0002) and exposes lookup(lat, lon) → admin_id | null.
 * TypedArray views that may be at non-4-byte-aligned offsets (rank table,
 * Morton directory) are copied via slice() to guarantee alignment.
 */

const GRID_COLS         = 1440;
const GRID_ROWS         = 720;
const SENTINEL_BOUNDARY = 0xFFFF;
const SENTINEL_OCEAN    = 0xFFFE;
const BLOCK_SIZE        = 64;
const BLOCK_RECORDS     = 10;
const RECORD_SIZE       = 6;  // uint32 morton + uint16 admin_id

// Byte popcount lookup table
const _PC8 = new Uint8Array(256);
for (let i = 1; i < 256; i++) _PC8[i] = (i & 1) + _PC8[i >> 1];

// Morton: spread bits of 16-bit int into even positions
function _spread(v) {
  v = (v | (v << 8)) & 0x00FF00FF;
  v = (v | (v << 4)) & 0x0F0F0F0F;
  v = (v | (v << 2)) & 0x33333333;
  v = (v | (v << 1)) & 0x55555555;
  return v >>> 0;
}

function _morton(lat, lon) {
  const lq = ((lat + 90)  / 180 * 4096) & 0xFFF;
  const aq = ((lon + 180) / 360 * 4096) & 0xFFF;
  return (_spread(lq) | (_spread(aq) << 1)) >>> 0;
}

export class Z0 {
  constructor(buffer) {
    const dv = new DataView(buffer);

    const magic = String.fromCharCode(...new Uint8Array(buffer, 0, 8));
    if (magic !== 'RGEO0002') throw new Error(`z0: bad magic "${magic}"`);

    // Header layout (uint32 LE, starting at byte 8):
    //  8  version    12  timestamp
    // 16  bitmapOff  20  rankOff   24  valuesOff  28  landCellCount
    // 32  mBlockOff  36  mDirOff   40  mRecCount  44  mBlockCount
    // 48  adminOff   52  nameOff   56  reserved(8)
    const bitmapOff   = dv.getUint32(16, true);
    const rankOff     = dv.getUint32(20, true);
    const valuesOff   = dv.getUint32(24, true);
    const landCells   = dv.getUint32(28, true);
    const mBlockOff   = dv.getUint32(32, true);
    const mDirOff     = dv.getUint32(36, true);
    const mBlockCount = dv.getUint32(44, true);

    const bitmapBytes   = Math.ceil(GRID_COLS * GRID_ROWS / 8);
    const numRankBlocks = Math.ceil(GRID_COLS * GRID_ROWS / 512);

    this._dv        = dv;
    this._mBlockOff = mBlockOff;

    // Bitmap: Uint8Array, no alignment constraint
    this._bitmap = new Uint8Array(buffer, bitmapOff, bitmapBytes);

    // Rank table: Uint32Array — offset may not be 4-byte aligned, so copy
    this._rank = new Uint32Array(buffer.slice(rankOff, rankOff + numRankBlocks * 4));

    // Grid values: Uint16Array — offset may not be 2-byte aligned, so copy
    this._values = new Uint16Array(buffer.slice(valuesOff, valuesOff + landCells * 2));

    // Morton directory: Uint32Array — copy to ensure alignment
    this._mDir = new Uint32Array(buffer.slice(mDirOff, mDirOff + mBlockCount * 4));

    // Pre-compute per-cell rank so lookup() needs no loop
    const cellCount = GRID_COLS * GRID_ROWS;
    const rankAtCell = new Int32Array(cellCount);
    let r = 0;
    for (let i = 0; i < cellCount; i++) {
      if ((this._bitmap[i >> 3] >> (i & 7)) & 1) rankAtCell[i] = r++;
      else rankAtCell[i] = -1;
    }
    this._rankAtCell = rankAtCell;
  }

  /** Returns admin_id (0–65533) or null for ocean/unclassified. */
  lookup(lat, lon) {
    lat = Math.max(-90,  Math.min(90,  lat));
    lon = Math.max(-180, Math.min(180, lon));

    // Layer 0: coarse 0.25° grid
    const col = Math.min(GRID_COLS - 1, Math.max(0, ((lon + 180) / 0.25) | 0));
    const row = Math.min(GRID_ROWS - 1, Math.max(0, ((90 - lat)  / 0.25) | 0));
    const idx = row * GRID_COLS + col;

    const rank = this._rankAtCell[idx];
    if (rank < 0) return null;  // ocean

    const v = this._values[rank];
    if (v === SENTINEL_OCEAN) return null;
    if (v <= 0xFFFD)          return v;          // interior fast path

    // Layer 1: Morton boundary table
    const m = _morton(lat, lon);
    return this._blockSearch(m) ?? this._blockSearch(m >>> 2);
  }

  _blockSearch(m) {
    const dir = this._mDir;
    const n   = dir.length;
    if (!n) return null;

    let lo = 0, hi = n - 1;
    while (lo < hi) {
      const mid = (lo + hi + 1) >>> 1;
      if (dir[mid] <= m) lo = mid; else hi = mid - 1;
    }
    if (dir[lo] > m) return null;

    const base = this._mBlockOff + lo * BLOCK_SIZE;
    const dv   = this._dv;
    for (let i = 0; i < BLOCK_RECORDS; i++) {
      const off = base + i * RECORD_SIZE;
      const rm  = dv.getUint32(off, true);
      if (rm > m)   break;
      if (rm === m) return dv.getUint16(off + 4, true);
    }
    return null;
  }
}

export async function loadZ0(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch ${url}: ${res.status}`);
  return new Z0(await res.arrayBuffer());
}
