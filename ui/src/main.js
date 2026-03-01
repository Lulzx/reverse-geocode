/**
 * main.js — Unified Atlas.
 *
 * Offline world map with a geocoder selector (Z0 / S2 / H3).
 * Pick one at startup; the appropriate binary is loaded and all
 * interactive features (hover, pin, explore, sparkline, counter) work
 * the same regardless of which geocoder is active.
 */

import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';

import { Z0 } from './z0.js';
import { S2 } from './s2.js';
import { H3 } from './h3.js';
import { UI } from './ui.js';

// ── Service Worker ────────────────────────────────────────────────────────────
if ('serviceWorker' in navigator) {
  navigator.serviceWorker.register('sw.js').then(() => {
    const poll = () => navigator.serviceWorker.controller
      ? ui.setOffline(true) : setTimeout(poll, 700);
    setTimeout(poll, 900);
  }).catch(() => {});
}

// ── Map ───────────────────────────────────────────────────────────────────────
const map = new maplibregl.Map({
  container: 'map',
  style: {
    version: 8,
    sources: {},
    layers: [{ id: 'bg', type: 'background', paint: { 'background-color': '#000' } }],
  },
  center: [10, 20],
  zoom: 2,
  maxPitch: 0,
  attributionControl: false,
});

map.addControl(new maplibregl.AttributionControl({ compact: true }), 'bottom-right');
map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'bottom-right');

// ── State ─────────────────────────────────────────────────────────────────────
const ui       = new UI();
let geocoder   = null;
let geoType    = null;    // 'z0' | 's2' | 'h3'
let adminMap   = null;
let geojson    = null;
let lastHlId   = null;
let mapReady   = false;
let dataReady  = false;
let rafPending = false;
let pendingX   = 0;
let pendingY   = 0;

// ── Explore callback ──────────────────────────────────────────────────────────
ui.onExplore = () => {
  if (!geojson || !mapReady) return;
  ui.unpin();
  const features = geojson.features;
  const f = features[Math.floor(Math.random() * features.length)];
  const ring = f.geometry.type === 'Polygon'
    ? f.geometry.coordinates[0]
    : f.geometry.coordinates[0][0];
  let sumLng = 0, sumLat = 0;
  for (const [lng, lat] of ring) { sumLng += lng; sumLat += lat; }
  const center = [sumLng / ring.length, sumLat / ring.length];
  map.flyTo({ center, zoom: 5 });
  setHl(f.id);
  const p = adminMap.get(f.id);
  if (p) {
    const cx = window.innerWidth / 2, cy = window.innerHeight / 2;
    const coords = `${fmtCoord(center[1], 'N', 'S')}  ${fmtCoord(center[0], 'E', 'W')}`;
    ui.update({ flag: toFlag(p.country), country: p.country, adm1: p.adm1, adm2: p.adm2, ms: 0, coords }, cx, cy, coords);
    ui.addCountry(p.country);
  }
};

// ── Map ready ─────────────────────────────────────────────────────────────────
map.on('load', () => { mapReady = true; if (dataReady) attach(); });

// ── Geocoder selector ─────────────────────────────────────────────────────────
geoType = await new Promise(resolve => {
  document.querySelectorAll('#selector button[data-geo]').forEach(btn => {
    btn.addEventListener('click', () => {
      const sel = document.getElementById('selector');
      sel.classList.add('done');
      sel.addEventListener('transitionend', () => sel.remove(), { once: true });
      resolve(btn.dataset.geo);
    });
  });
});

// ── Show loading, set geocoder label ─────────────────────────────────────────
const _loading = document.getElementById('loading');
_loading.style.display = '';
document.getElementById('loading-name').textContent =
  geoType === 'z0' ? 'Z0 — Morton' : geoType === 's2' ? 'S2 — H3 cells' : 'H3 — H3 cells';

// ── Progress bar ──────────────────────────────────────────────────────────────
const _fill = document.getElementById('progress-fill');
function setProgress(pct) { _fill.style.width = `${(pct * 100).toFixed(1)}%`; }

async function fetchBuffer(url, onProgress) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`fetch ${url}: ${res.status}`);
  const total = Number(res.headers.get('content-length') || 0);
  const reader = res.body.getReader();
  const chunks = [];
  let received = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
    received += value.length;
    onProgress(received, total || received);
  }
  const out = new Uint8Array(received);
  let off = 0;
  for (const c of chunks) { out.set(c, off); off += c.length; }
  return out.buffer;
}

// ── Data URL ──────────────────────────────────────────────────────────────────
const DATA = (location.hostname === 'localhost' || location.hostname === '127.0.0.1')
  ? '/data'
  : 'https://pub-4ad65c005bad4ef08b4bca5befc474b8.r2.dev';

// ── Load data based on chosen geocoder ───────────────────────────────────────
const geoBin = {
  z0: [`${DATA}/z0_geo.bin`],
  s2: [`${DATA}/s2_geo.bin`],
  h3: [`${DATA}/h3_geo.bin`, `${DATA}/h3_names.json`],
}[geoType];

const nLoads = 1 + geoBin.length;
const prog   = Array.from({ length: nLoads }, () => ({ r: 0, t: 0 }));
function updateBar() {
  const r = prog.reduce((s, p) => s + p.r, 0);
  const t = prog.reduce((s, p) => s + p.t, 0);
  if (t > 0) setProgress(r / t);
}

const loaders = [
  fetchBuffer(`${DATA}/adm2_render.geojson`, (r, t) => { prog[0] = { r, t }; updateBar(); })
    .then(buf => JSON.parse(new TextDecoder().decode(buf))),
  fetchBuffer(geoBin[0], (r, t) => { prog[1] = { r, t }; updateBar(); }),
];
if (geoType === 'h3') {
  loaders.push(
    fetchBuffer(geoBin[1], (r, t) => { prog[2] = { r, t }; updateBar(); })
      .then(buf => JSON.parse(new TextDecoder().decode(buf))),
  );
}

const [gj, bin, names] = await Promise.all(loaders);

for (const f of gj.features) f.properties._c = countryFill(f.properties.country);
geojson  = gj;
adminMap = new Map(gj.features.map(f => [f.id, f.properties]));

if (geoType === 'z0')      geocoder = new Z0(bin);
else if (geoType === 's2') geocoder = new S2(bin);
else                       geocoder = new H3(bin, names);

dataReady = true;
if (mapReady) attach();

// ── Attach GeoJSON layers ─────────────────────────────────────────────────────
function attach() {
  map.addSource('adm2', { type: 'geojson', data: geojson, generateId: false });

  map.addLayer({
    id: 'land', type: 'fill', source: 'adm2',
    paint: { 'fill-color': ['get', '_c'], 'fill-opacity': 1 },
  });
  map.addLayer({
    id: 'dividers', type: 'line', source: 'adm2',
    minzoom: 4,
    paint: {
      'line-color': '#000',
      'line-width': ['interpolate', ['linear'], ['zoom'], 4, 0.15, 8, 0.5],
      'line-opacity': ['interpolate', ['linear'], ['zoom'], 4, 0.25, 8, 0.7],
    },
  });
  map.addLayer({
    id: 'hl', type: 'fill', source: 'adm2',
    paint: {
      'fill-color': '#fff',
      'fill-opacity': ['case', ['boolean', ['feature-state', 'on'], false], 0.12, 0],
    },
  });
  map.addLayer({
    id: 'hl-border', type: 'line', source: 'adm2',
    paint: {
      'line-color': '#fff',
      'line-width':   ['case', ['boolean', ['feature-state', 'on'], false], 1, 0],
      'line-opacity': ['case', ['boolean', ['feature-state', 'on'], false], 0.55, 0],
    },
  });

  const loading = document.getElementById('loading');
  loading.classList.add('done');
  loading.addEventListener('transitionend', () => loading.remove(), { once: true });
}

// ── Interaction ───────────────────────────────────────────────────────────────
const canvas = map.getCanvas();

canvas.addEventListener('mousemove', e => {
  pendingX = e.clientX; pendingY = e.clientY;
  if (!rafPending) { rafPending = true; requestAnimationFrame(frame); }
}, { passive: true });

canvas.addEventListener('mouseleave', () => { ui.unpin(); ui.hide(); clearHl(); }, { passive: true });

canvas.addEventListener('click', () => { ui.togglePin(); }, { passive: true });

canvas.addEventListener('touchend', e => {
  if (e.changedTouches.length !== 1) return;
  if (e.timeStamp - (map._ts || 0) > 280) return;
  e.preventDefault();
  const t = e.changedTouches[0];
  runQuery(t.clientX, t.clientY);
}, { passive: false });

canvas.addEventListener('touchstart', e => { map._ts = e.timeStamp; }, { passive: true });

function frame() {
  rafPending = false;
  if (geocoder && mapReady) runQuery(pendingX, pendingY);
}

function runQuery(cx, cy) {
  const rect = canvas.getBoundingClientRect();
  const x    = cx - rect.left;
  const y    = cy - rect.top;
  const ll   = map.unproject([x, y]);
  const { lat, lng } = ll;
  const coords = `${fmtCoord(lat, 'N', 'S')}  ${fmtCoord(lng, 'E', 'W')}`;

  const t0 = performance.now();

  if (geoType === 'h3') {
    const result   = geocoder.lookup(lat, lng);
    const ms       = performance.now() - t0;
    const rendered = map.queryRenderedFeatures([x, y], { layers: ['land'] });
    const hlId     = rendered[0]?.id ?? null;
    setHl(hlId);
    if (result) {
      const geoP    = hlId !== null ? adminMap.get(hlId) : null;
      const country = geoP?.country || result.country;
      const adm1    = geoP?.adm1    || result.adm1;
      const adm2    = result.adm2   || geoP?.adm2 || '';
      ui.update({ flag: toFlag(country), country, adm1, adm2, ms, coords }, cx, cy, coords);
      ui.addCountry(country);
      return;
    }

  } else if (geoType === 's2') {
    const adminId  = geocoder.lookup(lat, lng);
    const ms       = performance.now() - t0;
    const rendered = map.queryRenderedFeatures([x, y], { layers: ['land'] });
    setHl(rendered[0]?.id ?? null);
    if (adminId !== null) {
      const p = adminMap.get(adminId);
      if (p) {
        ui.update({ flag: toFlag(p.country), country: p.country, adm1: p.adm1, adm2: p.adm2, ms, coords }, cx, cy, coords);
        ui.addCountry(p.country);
        return;
      }
    }

  } else {  // z0
    const id = geocoder.lookup(lat, lng);
    const ms = performance.now() - t0;
    setHl(id);
    if (id !== null) {
      const p = adminMap.get(id);
      if (p) {
        ui.update({ flag: toFlag(p.country), country: p.country, adm1: p.adm1, adm2: p.adm2, ms, coords }, cx, cy, coords);
        ui.addCountry(p.country);
        return;
      }
    }
  }

  ui.update(null, cx, cy, coords);
}

// ── Highlight ─────────────────────────────────────────────────────────────────
function setHl(id) {
  if (lastHlId === id) return;
  clearHl();
  if (id !== null) { map.setFeatureState({ source: 'adm2', id }, { on: true }); lastHlId = id; }
}

function clearHl() {
  if (lastHlId !== null) {
    map.setFeatureState({ source: 'adm2', id: lastHlId }, { on: false });
    lastHlId = null;
  }
}

// ── Per-country fill: hash alpha-3 → narrow dark hue band ────────────────────
function countryFill(a3) {
  let h = 0;
  for (let i = 0; i < (a3?.length ?? 0); i++) h = (Math.imul(h, 31) + a3.charCodeAt(i)) | 0;
  const hue  = ((h >>> 0) % 40) + 195;
  const sat  = 20 + ((h >>> 8)  & 0xF);
  const lite = 12 + ((h >>> 12) & 0x7);
  return `hsl(${hue},${sat}%,${lite}%)`;
}

// ── Flag emoji ────────────────────────────────────────────────────────────────
const A3A2 = {
  AFG:'AF',ALB:'AL',DZA:'DZ',AND:'AD',AGO:'AO',ATG:'AG',ARG:'AR',ARM:'AM',
  AUS:'AU',AUT:'AT',AZE:'AZ',BHS:'BS',BHR:'BH',BGD:'BD',BRB:'BB',BLR:'BY',
  BEL:'BE',BLZ:'BZ',BEN:'BJ',BTN:'BT',BOL:'BO',BIH:'BA',BWA:'BW',BRA:'BR',
  BRN:'BN',BGR:'BG',BFA:'BF',BDI:'BI',CPV:'CV',KHM:'KH',CMR:'CM',CAN:'CA',
  CAF:'CF',TCD:'TD',CHL:'CL',CHN:'CN',COL:'CO',COM:'KM',COD:'CD',COG:'CG',
  CRI:'CR',CIV:'CI',HRV:'HR',CUB:'CU',CYP:'CY',CZE:'CZ',DNK:'DK',DJI:'DJ',
  DOM:'DO',ECU:'EC',EGY:'EG',SLV:'SV',GNQ:'GQ',ERI:'ER',EST:'EE',SWZ:'SZ',
  ETH:'ET',FJI:'FJ',FIN:'FI',FRA:'FR',GAB:'GA',GMB:'GM',GEO:'GE',DEU:'DE',
  GHA:'GH',GRC:'GR',GTM:'GT',GIN:'GN',GNB:'GW',GUY:'GY',HTI:'HT',HND:'HN',
  HUN:'HU',ISL:'IS',IND:'IN',IDN:'ID',IRN:'IR',IRQ:'IQ',IRL:'IE',ISR:'IL',
  ITA:'IT',JAM:'JM',JPN:'JP',JOR:'JO',KAZ:'KZ',KEN:'KE',PRK:'KP',KOR:'KR',
  KWT:'KW',KGZ:'KG',LAO:'LA',LVA:'LV',LBN:'LB',LSO:'LS',LBR:'LR',LBY:'LY',
  LIE:'LI',LTU:'LT',LUX:'LU',MDG:'MG',MWI:'MW',MYS:'MY',MDV:'MV',MLI:'ML',
  MLT:'MT',MRT:'MR',MUS:'MU',MEX:'MX',MDA:'MD',MCO:'MC',MNG:'MN',MNE:'ME',
  MAR:'MA',MOZ:'MZ',MMR:'MM',NAM:'NA',NPL:'NP',NLD:'NL',NZL:'NZ',NIC:'NI',
  NER:'NE',NGA:'NG',MKD:'MK',NOR:'NO',OMN:'OM',PAK:'PK',PAN:'PA',PNG:'PG',
  PRY:'PY',PER:'PE',PHL:'PH',POL:'PL',PRT:'PT',QAT:'QA',ROU:'RO',RUS:'RU',
  RWA:'RW',KNA:'KN',LCA:'LC',VCT:'VC',WSM:'WS',STP:'ST',SAU:'SA',SEN:'SN',
  SRB:'RS',SLE:'SL',SGP:'SG',SVK:'SK',SVN:'SI',SLB:'SB',SOM:'SO',ZAF:'ZA',
  SSD:'SS',ESP:'ES',LKA:'LK',SDN:'SD',SUR:'SR',SWE:'SE',CHE:'CH',SYR:'SY',
  TWN:'TW',TJK:'TJ',TZA:'TZ',THA:'TH',TLS:'TL',TGO:'TG',TON:'TO',TTO:'TT',
  TUN:'TN',TUR:'TR',TKM:'TM',TUV:'TV',UGA:'UG',UKR:'UA',ARE:'AE',GBR:'GB',
  USA:'US',URY:'UY',UZB:'UZ',VUT:'VU',VEN:'VE',VNM:'VN',YEM:'YE',ZMB:'ZM',
  ZWE:'ZW',XKX:'XK',
};

function toFlag(a3) {
  const a2 = A3A2[a3?.toUpperCase()];
  if (!a2) return '';
  const a = a2.charCodeAt(0) - 65, b = a2.charCodeAt(1) - 65;
  return (a < 0 || a > 25 || b < 0 || b > 25)
    ? '' : String.fromCodePoint(0x1F1E6 + a, 0x1F1E6 + b);
}

// ── Coordinate formatting ─────────────────────────────────────────────────────
function fmtCoord(v, pos, neg) {
  return `${Math.abs(v).toFixed(4)}° ${v >= 0 ? pos : neg}`;
}
