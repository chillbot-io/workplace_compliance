"""
scripts/reset_monthly_usage.py — Reset the denormalized current_usage counter on API keys.
Runs 1st of each month at midnight via cron.
Note: this is display-only. Quota enforcement uses api_usage row counting.
"""

import asyncio
import os

import asyncpg


async def reset():
    pool = await asyncpg.create_pool(os.environ["PG_DSN"])
    async with pool.acquire() as con:
        result = await con.execute("""
            UPDATE api_keys SET current_usage = 0
            WHERE current_usage > 0
        """)
        count = int(result.split()[-1]) if result else 0
        print(f"Monthly usage reset: {count} keys zeroed")
    await pool.close()


if __name__ == "__main__":
    asyncio.run(reset())
