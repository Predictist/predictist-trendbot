from datetime import datetime, timezone
import requests
from typing import Iterable
from app.core.db import db

def _poly_market_uid(vendor_id: str) -> str:
    return f"poly_{vendor_id}"

def upsert_markets(markets: Iterable[dict]):
    sql = """
    INSERT INTO market (market_uid, vendor, question, category, created_at, close_time, url, status)
    VALUES (%(market_uid)s, 'polymarket', %(question)s, %(category)s, %(created_at)s, %(close_time)s, %(url)s, %(status)s)
    ON CONFLICT (market_uid) DO UPDATE SET
      question = EXCLUDED.question,
      category = EXCLUDED.category,
      close_time = EXCLUDED.close_time,
      url = EXCLUDED.url,
      status = EXCLUDED.status;
    """
    rows = []
    for m in markets:
        rows.append({
            "market_uid": _poly_market_uid(m["id"]),
            "question": m.get("question") or m.get("title") or "",
            "category": (m.get("category") or "").lower() or None,
            "created_at": datetime.fromtimestamp(m.get("createdAt", m.get("created_at", 0)) / 1000, tz=timezone.utc),
            "close_time": datetime.fromtimestamp(m.get("endDate", m.get("endTime", 0)) / 1000, tz=timezone.utc) if m.get("endDate") or m.get("endTime") else None,
            "url": m.get("url") or f"https://polymarket.com/market/{m['id']}",
            "status": (m.get("status") or "open").lower()
        })
    return sql, rows

async def ingest_markets(markets_url: str):
    resp = requests.get(markets_url, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    markets = data if isinstance(data, list) else data.get("markets", [])
    sql, rows = upsert_markets(markets)

    async with db.cursor() as cur:
        await cur.executemany(sql, rows)

    return [m["id"] for m in markets]

async def ingest_ticks(ticks_url_template: str, vendor_ids: list[str]):
    # Pull last 48 hours of hourly candles per market
    insert_sql = """
    INSERT INTO price_tick (market_uid, ts, price, volume_24h, liquidity)
    VALUES (%(market_uid)s, %(ts)s, %(price)s, %(volume_24h)s, %(liquidity)s)
    ON CONFLICT (market_uid, ts) DO UPDATE SET
      price = EXCLUDED.price,
      volume_24h = EXCLUDED.volume_24h,
      liquidity = EXCLUDED.liquidity;
    """
    all_rows = []
    for vid in vendor_ids:
        url = ticks_url_template.format(market_id=vid)
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            continue
        candles = r.json() or []
        for c in candles:
            # Polymarket candle schema can vary; assume:
            # c['t'] (ms), c['close'], c['volume'] (per hour), optional liquidity
            ts = datetime.fromtimestamp(c.get('t', 0)/1000, tz=timezone.utc)
            price = c.get('close')
            vol = c.get('volume')
            liq = c.get('liquidity')  # may be missing
            all_rows.append({
                "market_uid": _poly_market_uid(vid),
                "ts": ts,
                "price": float(price) if price is not None else None,
                "volume_24h": float(vol) if vol is not None else None,
                "liquidity": float(liq) if liq is not None else None
            })
    if all_rows:
        async with db.cursor() as cur:
            await cur.executemany(insert_sql, all_rows)
