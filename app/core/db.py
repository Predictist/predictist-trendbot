import psycopg
from contextlib import asynccontextmanager

class DB:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = None

    async def connect(self):
        # psycopg3 built-in pool
        self.pool = await psycopg.AsyncConnection.connect(self.dsn)

    async def close(self):
        if self.pool:
            await self.pool.close()

    @asynccontextmanager
    async def cursor(self):
        async with self.pool.cursor() as cur:
            yield cur

db: DB | None = None

async def init_db(dsn: str):
    global db
    db = DB(dsn)
    await db.connect()
    return db
