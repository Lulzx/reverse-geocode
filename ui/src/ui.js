/**
 * ui.js — Tooltip, offline dot, counter, explore button, sparkline.
 *
 * Uber design language: solid black, precise weight hierarchy,
 * no blur, no glass, no decorative border. Information only.
 */

export class UI {
  constructor() {
    this._tip         = null;
    this._dot         = null;
    this._sparkCanvas = null;
    this._spark       = [];
    this._pinned      = false;
    this._seen        = new Set();
    this._counter     = null;
    this._exploreBtn  = null;
    this.onExplore    = null;
    this._build();
  }

  /**
   * @param {object} p  — { flag, country, adm1, adm2, ms, coords }
   *                       adm1/adm2 may be empty.
   *                       Pass null to show ocean state.
   */
  update(p, cx, cy, coords) {
    if (this._pinned) return;

    const el = this._tip;

    if (!p) {
      el.innerHTML =
        `<span class="t-ocean">Ocean</span>` +
        `<span class="t-meta"><span class="t-coords">${esc(coords)}</span></span>`;
    } else {
      this._spark.push(p.ms);
      if (this._spark.length > 30) this._spark.shift();

      const region = [p.adm1, p.adm2].filter(Boolean).join(' · ');
      el.innerHTML =
        `<span class="t-flag">${p.flag}</span>` +
        `<span class="t-country">${esc(p.country)}</span>` +
        (region ? `<span class="t-region">${esc(region)}</span>` : '') +
        `<span class="t-meta">` +
        `<span class="t-coords">${esc(coords)}</span>` +
        `<span class="t-perf"><span class="t-ms">${fmtMs(p.ms)}</span></span></span>`;

      const perf = el.querySelector('.t-perf');
      if (perf) perf.appendChild(this._sparkCanvas);
      this._drawSpark();
    }

    const m  = 12;
    const tw = el.offsetWidth  || 210;
    const th = el.offsetHeight || 72;
    const vw = window.innerWidth;
    const vh = window.innerHeight;

    let tx = cx + 18, ty = cy - 8;
    if (tx + tw > vw - m) tx = cx - tw - 18;
    if (ty + th > vh - m) ty = cy - th - 8;
    ty = Math.max(m, ty);
    tx = Math.max(m, tx);

    el.style.transform  = `translate(${tx}px,${ty}px)`;
    el.style.opacity    = '1';
    el.style.visibility = 'visible';
  }

  hide() {
    if (this._pinned) return;
    this._tip.style.opacity    = '0';
    this._tip.style.visibility = 'hidden';
  }

  togglePin() {
    this._pinned = !this._pinned;
    this._tip.classList.toggle('pinned', this._pinned);
  }

  unpin() {
    if (!this._pinned) return;
    this._pinned = false;
    this._tip.classList.remove('pinned');
  }

  addCountry(code) {
    if (!code) return;
    const prev = this._seen.size;
    this._seen.add(code);
    if (this._seen.size !== prev) {
      this._counter.textContent = `${this._seen.size} countries`;
      this._counter.classList.add('flash');
      setTimeout(() => this._counter.classList.remove('flash'), 400);
    }
  }

  setOffline(ready) {
    this._dot.dataset.s = ready ? '1' : '0';
  }

  _drawSpark() {
    const cv  = this._sparkCanvas;
    const ctx = cv.getContext('2d');
    ctx.clearRect(0, 0, cv.width, cv.height);
    if (this._spark.length < 2) return;
    const max = Math.max(...this._spark, 0.001);
    const w   = cv.width / this._spark.length;
    ctx.fillStyle = '#1db954';
    for (let i = 0; i < this._spark.length; i++) {
      const h = Math.max(1, (this._spark[i] / max) * cv.height);
      ctx.fillRect(i * w, cv.height - h, Math.max(1, w - 0.5), h);
    }
  }

  _build() {
    const tip = document.createElement('div');
    tip.id = 'tip';
    document.body.appendChild(tip);
    this._tip = tip;

    const cv = document.createElement('canvas');
    cv.id = 't-spark';
    cv.width  = 40;
    cv.height = 12;
    this._sparkCanvas = cv;

    const ctr = document.createElement('div');
    ctr.id = 'country-counter';
    ctr.textContent = '0 countries';
    document.body.appendChild(ctr);
    this._counter = ctr;

    const btn = document.createElement('button');
    btn.id = 'explore-btn';
    btn.title = 'Fly to a random location';
    btn.textContent = '⟳';
    btn.addEventListener('click', () => { if (this.onExplore) this.onExplore(); });
    document.body.appendChild(btn);
    this._exploreBtn = btn;

    const dot = document.createElement('div');
    dot.id = 'offline-dot';
    dot.dataset.s = '0';
    document.body.appendChild(dot);
    this._dot = dot;
  }
}

function fmtMs(ms) {
  return ms < 0.1 ? `${(ms * 1000).toFixed(0)}µs` : `${ms.toFixed(2)}ms`;
}

function esc(s) {
  return String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

const css = `
  #tip {
    position: fixed;
    top: 0; left: 0;
    pointer-events: none;
    z-index: 100;
    opacity: 0;
    visibility: hidden;
    will-change: transform;
    transition: opacity 0.06s linear;

    background: #000;
    color: #fff;
    padding: 12px 14px 10px;
    min-width: 160px;
    max-width: 240px;

    display: flex;
    flex-direction: column;
    gap: 3px;
  }

  #tip.pinned { border-left: 2px solid #333; }

  #tip .t-flag    { font-size: 18px; line-height: 1; margin-bottom: 4px; }
  #tip .t-country { font-size: 15px; font-weight: 600; letter-spacing: -0.3px;
                    line-height: 1.2; color: #fff; }
  #tip .t-region  { font-size: 12px; color: #888; line-height: 1.3; }
  #tip .t-ocean   { font-size: 13px; color: #555; font-style: italic; }
  #tip .t-meta    { display: flex; justify-content: space-between; align-items: center;
                    margin-top: 6px; padding-top: 6px;
                    border-top: 1px solid #222; }
  #tip .t-coords  { font-size: 10px; color: #666; font-family: 'SF Mono', 'Menlo', monospace; }
  #tip .t-ms      { font-size: 10px; color: #1db954; font-family: 'SF Mono', 'Menlo', monospace; }
  #tip .t-perf    { display: flex; align-items: center; gap: 4px; }
  #t-spark        { display: block; }

  /* Offline indicator — bottom-left */
  #offline-dot {
    position: fixed;
    bottom: 28px; left: 14px;
    z-index: 100;
    width: 6px; height: 6px;
    border-radius: 50%;
    transition: background 0.4s;
  }
  #offline-dot[data-s="0"] { background: #333; }
  #offline-dot[data-s="1"] { background: #1db954; }

  /* Countries explored counter — top-left */
  #country-counter {
    position: fixed;
    top: 14px; left: 14px;
    z-index: 100;
    font-size: 10px; color: #333;
    font-family: 'SF Mono', 'Menlo', monospace;
    pointer-events: none;
    transition: color 0.3s;
  }
  #country-counter.flash { color: #888; }

  /* Explore button — bottom-left, above offline dot */
  #explore-btn {
    position: fixed;
    bottom: 44px; left: 9px;
    z-index: 100;
    width: 22px; height: 22px;
    background: none; border: none;
    color: #444;
    font-size: 16px; cursor: pointer;
    padding: 0; line-height: 22px; text-align: center;
    transition: color 0.2s;
  }
  #explore-btn:hover { color: #888; }

  .maplibregl-ctrl-logo   { display: none !important; }
  .maplibregl-ctrl-attrib { opacity: 0.3 !important; }
`;

const s = document.createElement('style');
s.textContent = css;
document.head.appendChild(s);
