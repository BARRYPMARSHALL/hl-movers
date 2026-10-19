#!/usr/bin/env node
'use strict';
/**
 * Snapshot 1h/4h change for every live Hyperliquid perp into data.json.
 *
 * WHY THIS EXISTS INSTEAD OF THE PAGE DOING IT
 * --------------------------------------------
 * 1h/4h need one candle request PER MARKET — 178 of them. Measured against the public info API:
 * ~2.5s per request, and 2 req/s is sustainable while 2.5 req/s gets throttled. Doing that in every
 * visitor's browser means every visitor earns a 429, and the page quietly stops getting data.
 *
 * So one scheduled job pays that cost every 15 minutes and writes a small static file. The page
 * still calls the info API live for price, 24h, volume and OI (ONE request), and reads this file
 * for the intraday columns. Fast for the visitor, one sweep per quarter hour against the venue.
 *
 * Written to be run by GitHub Actions, but it runs anywhere node does.
 */

const fs = require('node:fs');
const path = require('node:path');

const API = 'https://api.hyperliquid.xyz/info';
const OUT = process.argv[2] || path.join(__dirname, '..', 'data.json');
const HOUR = 3600e3;
const GAP_MS = 300;          // ~3.3 req/s ceiling; measured-safe is 2/s, so this relies on backoff
const CONC = 4;
const MAX_429_BEFORE_GIVING_UP = 40;

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function post(body, tries = 3) {
  let last;
  for (let a = 1; a <= tries; a++) {
    const res = await fetch(API, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body), signal: AbortSignal.timeout(30000),
    });
    if (res.status === 429) { last = new Error('HTTP 429'); await sleep(2000 * a); continue; }
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
  }
  throw last;
}

const closeAt = (cands, target) => {
  let hit = null;
  for (const c of cands) { if (c.t <= target) hit = c; else break; }
  return hit ? Number(hit.c) : null;
};

(async () => {
  const t0 = Date.now();
  const [meta, ctxs] = await post({ type: 'metaAndAssetCtxs' });
  const uni = meta.universe || [];

  const coins = [];
  for (let i = 0; i < uni.length; i++) {
    const u = uni[i];
    if (u.isDelisted) continue;
    if (String(u.dex ?? 'main') !== 'main') continue;   // primary perp dex only
    const markPx = Number((ctxs[i] || {}).markPx || 0);
    coins.push({ coin: u.name, markPx });
  }
  // most liquid first, so if the sweep is cut short the rows that matter are already priced
  coins.sort((a, b) => (b.markPx ? 1 : 0) - (a.markPx ? 1 : 0));
  console.log(`  ${coins.length} live main-dex markets`);

  const now = Date.now();
  const startTime = now - (4 * HOUR + 15 * 60e3);
  const out = {};
  const queue = [...coins];
  let done = 0, failed = 0, gap = GAP_MS, lastSlot = 0, throttleStreak = 0;
  const t1 = Date.now();

  const slot = async () => {
    const t = Date.now();
    const at = Math.max(t, lastSlot + gap);
    lastSlot = at;
    if (at > t) await sleep(at - t);
  };

  const worker = async () => {
    while (queue.length) {
      if (throttleStreak > MAX_429_BEFORE_GIVING_UP) return;
      const m = queue.shift();
      await slot();
      try {
        const res = await fetch(API, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ type: 'candleSnapshot',
            req: { coin: m.coin, interval: '5m', startTime, endTime: Date.now() } }),
          signal: AbortSignal.timeout(30000),
        });
        if (res.status === 429) {
          throttleStreak++;
          gap = Math.min(gap * 2, 2500);
          queue.push(m);
          await sleep(1500);
          continue;
        }
        throttleStreak = 0;
        if (gap > GAP_MS) gap = Math.max(GAP_MS, gap * 0.9);
        const cands = res.ok ? await res.json() : null;
        if (Array.isArray(cands) && cands.length) {
          const last = Number(cands[cands.length - 1].c);
          const p1 = closeAt(cands, Date.now() - HOUR);
          const p4 = closeAt(cands, Date.now() - 4 * HOUR);
          out[m.coin] = {
            chg1h: p1 ? Number(((last / p1 - 1) * 100).toFixed(3)) : null,
            chg4h: p4 ? Number(((last / p4 - 1) * 100).toFixed(3)) : null,
          };
          if (out[m.coin].chg1h === null || out[m.coin].chg4h === null) failed++;
        } else { failed++; }
      } catch { failed++; }
      done++;
      if (done % 25 === 0) {
        console.log(`    ${done}/${coins.length}  ${((Date.now() - t1) / 1000).toFixed(0)}s  gap=${gap}ms`);
      }
    }
  };

  await Promise.all(Array.from({ length: CONC }, () => worker()));

  const priced = Object.values(out).filter(v => v.chg1h !== null && v.chg4h !== null).length;
  const payload = {
    at: new Date().toISOString(),
    universe: coins.length,
    priced,
    failed,
    markets: out,
  };
  fs.writeFileSync(OUT, JSON.stringify(payload));
  console.log(`  priced ${priced}/${coins.length} (${failed} unpriced) in ${((Date.now() - t0) / 1000).toFixed(0)}s`);
  console.log(`  -> ${OUT} (${(fs.statSync(OUT).size / 1024).toFixed(1)} KB)`);

  // If most of the sweep was lost to throttling, fail the job so the run is visibly red rather
  // than leaving a stale-but-plausible file in place.
  if (priced < coins.length * 0.6) {
    console.error(`  too few markets priced (${priced}/${coins.length}) — treating as a failed sweep`);
    process.exit(1);
  }
})().catch(e => { console.error('  FAILED:', e.message); process.exit(1); });
