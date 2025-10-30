from datetime import datetime, timedelta, timezone
import math
from app.core.db import db

WEIGHTS = dict(dvol=0.4, dprice=0.3, dliq=0.2, fresh=0.1)

def _freshness(age_hours: float) -> float:
    return math.exp(-age_hours/72.0)

async def compute_signals_and_scores(now: datetime | None = None, min_liq: float = 0.0):
    now = now or datetime.now(tz=timezone.utc)
    t24 = now - timedelta(hours=24)

    # Grab latest price_tick per market and the tick ~24h ago
    latest_sql = """
    WITH latest AS (
      SELECT DISTINCT ON (market_uid)
        market_uid, ts, price, volume_24h, liquidity
      FROM price_tick
      ORDER BY market_uid, ts DESC
    ),
    prev AS (
      SELECT pt.*
      FROM price_tick pt
      JOIN (
        SELECT market_uid, max(ts) AS ts24
        FROM price_tick
        WHERE ts <= %(t24)s
        GROUP BY market_uid
      ) x ON x.market_uid = pt.market_uid AND x.ts24 = pt.ts
    ),
    joined AS (
      SELECT
        m.market_uid, m.vendor, m.category, m.created_at,
        l.ts AS ts_now, l.price AS price_now, l.volume_24h AS vol_now, l.liquidity AS liq_now,
        p.ts AS ts_prev, p.price AS price_prev, p.volume_24h AS vol_prev, p.liquidity AS liq_prev
      FROM market m
      JOIN latest l ON l.market_uid = m.market_uid
      LEFT JOIN prev p ON p.market_uid = m.market_uid
      WHERE m.status = 'open'
    )
    SELECT * FROM joined;
    """

    async with db.cursor() as cur:
        await cur.execute(latest_sql, {"t24": t24})
        rows = await cur.fetchall()

    # Compute signals
    sig_insert = """
    INSERT INTO trend_signal (market_uid, run_at, dvol_24h, dprice_24h, dliq_24h, freshness)
    VALUES (%(market_uid)s, %(run_at)s, %(dvol)s, %(dprice)s, %(dliq)s, %(freshness)s)
    ON CONFLICT (market_uid, run_at) DO NOTHING;
    """

    score_insert = """
    INSERT INTO trend_score
      (market_uid, run_at, trend_score, rank, category, vendor, dvol_24h, dprice_24h, dliq_24h, price_now, volume_24h_now, liquidity_now)
    VALUES
      (%(market_uid)s, %(run_at)s, %(score)s, NULL, %(category)s, %(vendor)s, %(dvol)s, %(dprice)s, %(dliq)s, %(price_now)s, %(vol_now)s, %(liq_now)s)
    ON CONFLICT (market_uid, run_at) DO UPDATE SET
      trend_score = EXCLUDED.trend_score,
      dvol_24h = EXCLUDED.dvol_24h,
      dprice_24h = EXCLUDED.dprice_24h,
      dliq_24h = EXCLUDED.dliq_24h,
      price_now = EXCLUDED.price_now,
      volume_24h_now = EXCLUDED.volume_24h_now,
      liquidity_now = EXCLUDED.liquidity_now;
    """

    data_sig, data_score = [], []
    for (market_uid, vendor, category, created_at, ts_now, price_now, vol_now, liq_now,
         ts_prev, price_prev, vol_prev, liq_prev) in rows:

        # Deltas (guard against division by zero)
        dvol = (float(vol_now)/float(vol_prev) - 1.0) if (vol_now and vol_prev and vol_prev != 0) else None
        dprice = (float(price_now) - float(price_prev)) if (price_now is not None and price_prev is not None) else None
        dliq = (float(liq_now)/float(liq_prev) - 1.0) if (liq_now and liq_prev and liq_prev != 0) else None

        age_hours = (now - created_at).total_seconds()/3600.0 if created_at else 0.0
        fresh = _freshness(age_hours)

        # Normalize inputs: simple clamp & fill
        def nz(x, default=0.0):
            return float(x) if x is not None and math.isfinite(float(x)) else default

        # crude normalization caps (tune later)
        dvol_n = max(min(nz(dvol), 5.0), -0.95)     # cap +500% / -95%
        dprice_n = max(min(nz(dprice), 0.25), -0.25)
        dliq_n = max(min(nz(dliq), 5.0), -0.95)

        score_raw = (WEIGHTS["dvol"]*dvol_n +
                     WEIGHTS["dprice"]*dprice_n +
                     WEIGHTS["dliq"]*dliq_n +
                     WEIGHTS["fresh"]*fresh)

        # scale to 0..100 roughly
        score = max(0.0, min(100.0, (score_raw + 1.0) * 50.0))

        # Quality gate (optional): min liquidity now
        if min_liq and (liq_now is None or float(liq_now) < min_liq):
            continue

        data_sig.append({
            "market_uid": market_uid, "run_at": now, "dvol": dvol, "dprice": dprice, "dliq": dliq, "freshness": fresh
        })
        data_score.append({
            "market_uid": market_uid, "run_at": now, "score": score, "category": category, "vendor": vendor,
            "dvol": dvol, "dprice": dprice, "dliq": dliq, "price_now": price_now, "vol_now": vol_now, "liq_now": liq_now
        })

    async with db.cursor() as cur:
        if data_sig:
            await cur.executemany(sig_insert, data_sig)
        if data_score:
            await cur.executemany(score_insert, data_score)

    # Rank Top-N (per run)
    rank_sql = """
    WITH latest_run AS (
      SELECT max(run_at) AS run_at FROM trend_score
    ),
    ranked AS (
      SELECT market_uid, run_at, trend_score,
             ROW_NUMBER() OVER (ORDER BY trend_score DESC) AS rk
      FROM trend_score, latest_run
      WHERE trend_score.run_at = latest_run.run_at
    )
    UPDATE trend_score t
    SET rank = r.rk
    FROM ranked r
    WHERE t.market_uid = r.market_uid AND t.run_at = r.run_at;
    """
    async with db.cursor() as cur:
        await cur.execute(rank_sql)
