import asyncpg


async def reset_database(server_url: str, dbname: str) -> None:
    """Ensure dbname exists on server_url with an empty public schema."""
    admin = await asyncpg.connect(f"{server_url}/postgres")
    exists = await admin.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", dbname)
    if not exists:
        await admin.execute(f'CREATE DATABASE "{dbname}"')
    await admin.close()

    conn = await asyncpg.connect(f"{server_url}/{dbname}")
    await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
    await conn.close()
