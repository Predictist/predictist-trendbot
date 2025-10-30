from pydantic import BaseModel
from dotenv import load_dotenv
import os

load_dotenv()

class Settings(BaseModel):
    database_url: str = os.getenv("DATABASE_URL", "postgresql+psycopg://predictist:password@localhost:5432/predictist")
    ingest_interval_min: int = int(os.getenv("INGEST_INTERVAL_MIN", "30"))
    polymarket_markets_url: str = os.getenv("POLYMARKET_MARKETS_URL", "https://api.polymarket.com/markets")
    polymarket_ticks_url: str = os.getenv("POLYMARKET_TICKS_URL", "https://api.polymarket.com/markets/{market_id}/candlesticks?interval=1h&limit=48")
    enable_liquidity: bool = os.getenv("ENABLE_LIQUIDITY", "true").lower() == "true"

settings = Settings()
