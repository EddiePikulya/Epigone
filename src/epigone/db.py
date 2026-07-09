from importlib.resources import files

import asyncpg


async def create_pool(database_url: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(database_url)


async def apply_schema(pool: asyncpg.Pool) -> None:
    schema = files("epigone").joinpath("schema.sql").read_text()
    async with pool.acquire() as conn:
        await conn.execute(schema)
