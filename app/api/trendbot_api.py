from fastapi import FastAPI, Query
from app.core.config import settings
from app.core.db import init_db
from app.storage.trendbot_store import get_top_trends, get_category_momentum, get_timeline, search_markets

app = FastAPI(title="Predictist TrendBot API", version="0.1.0")

@app.on_event("startup")
async def startup():
    await init_db(settings.database_url)

@app.get("/api/trendbot/top")
async def api_top_trends(n: int = Query(10, ge=1, le=100), category: str | None = None):
    return await get_top_trends(n=n, category=category)

@app.get("/api/trendbot/category-momentum")
async def api_category_momentum(days: int = Query(7, ge=1, le=90)):
    return await get_category_momentum(days=days)

@app.get("/api/trendbot/timeline")
async def api_timeline(market_uid: str):
    return await get_timeline(market_uid)

@app.get("/api/trendbot/markets")
async def api_markets(search: str):
    return await search_markets(search)
