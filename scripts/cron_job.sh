#!/usr/bin/env bash
set -euo pipefail

# Activate your venv if needed
# source .venv/bin/activate

python - <<'PY'
import asyncio
from datetime import datetime, timezone
from app.core.config import settings
from app.core.db import init_db
from app.ingestion.polymarket_ingest import ingest_markets, ingest_ticks
from app.processing.trendbot_process import compute_signals_and_scores

async def main():
    await init_db(settings.database_url)

    # 1) Ingest Polymarket markets + ticks
    vendor_ids = await ingest_markets(settings.polymarket_markets_url)
    await ingest_ticks(settings.polymarket_ticks_url, vendor_ids)

    # 2) Compute signals + scores
    await compute_signals_and_scores(now=datetime.now(tz=timezone.utc), min_liq=0.0)

asyncio.run(main())
PY
