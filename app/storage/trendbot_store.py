from typing import Any, List, Dict
from app.core.db import db

async def get_top_trends(n: int = 10, category: str | None = None) -> List[Dict[str, Any]]:
    sql = """
    WITH latest_run AS (SELECT max(run_at) AS run_at FROM trend_score)
    SELECT t.rank, m.question AS market, t.trend_score, t.dvol_24h, t.dprice_24h, t.dliq_24h,
           t.price_now, t.volume_24h_now, t.liquidity_now,
           m.category, m.vendor, m.url, m.created_at, m.market_uid
    FROM trend_score t
    JOIN latest_run lr ON t.run_at = lr.run_at
    JOIN market m ON m.market_uid = t.market_uid
    WHERE (%(category)s IS NULL OR m.category = %(category)s)
    ORDER BY t.rank ASC
    LIMIT %(n)s;
    """
    async with db.cursor() as cur:
        await cur.execute(sql, {"n": n, "category": category})
        cols = [c[0] for c in cur.description]
        rows = await cur.fetchall()
    return [dict(zip(cols, r)) for r in rows]

async def get_category_momentum(days: int = 7):
    sql = """
    SELECT category, date, momentum_7d, avg_dvol_24h, avg_dprice_24h
    FROM trend_category_index
    WHERE date >= CURRENT_DATE - %(days)s::INT
    ORDER BY date DESC, category ASC;
    """
    async with db.cursor() as cur:
        await cur.execute(sql, {"days": days})
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in await cur.fetchall()]

async def get_timeline(market_uid: str):
    sql = """
    SELECT run_at AS timestamp, trend_score, dvol_24h, dprice_24h, dliq_24h
    FROM trend_score
    WHERE market_uid = %(uid)s
    ORDER BY run_at ASC;
    """
    async with db.cursor() as cur:
        await cur.execute(sql, {"uid": market_uid})
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in await cur.fetchall()]

async def search_markets(q: str):
    sql = """
    SELECT market_uid, vendor, question AS market, category, url, created_at, status
    FROM market
    WHERE question ILIKE %(q)s
    ORDER BY created_at DESC
    LIMIT 50;
    """
    async with db.cursor() as cur:
        await cur.execute(sql, {"q": f"%{q}%"})
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in await cur.fetchall()]
