"""
scripts/rotate_keys.py — Expire rotating_out API keys past their 48h NIST window.
Runs hourly via cron.
"""

import asyncio
import os

import asyncpg


async def rotate():
    pool = await asyncpg.create_pool(os.environ["PG_DSN"])
    async with pool.acquire() as con:
        # Expire rotating_out keys past their rotation window
        result = await con.execute("""
            UPDATE api_keys
            SET status = 'revoked'
            WHERE status = 'rotating_out'
              AND rotation_expires_at < NOW()
        """)
        count = int(result.split()[-1]) if result else 0
        if count > 0:
            print(f"Revoked {count} expired rotating keys")

        # Also expire keys past their expires_at
        result2 = await con.execute("""
            UPDATE api_keys
            SET status = 'revoked'
            WHERE status = 'active'
              AND expires_at IS NOT NULL
              AND expires_at < NOW()
        """)
        count2 = int(result2.split()[-1]) if result2 else 0
        if count2 > 0:
            print(f"Revoked {count2} expired keys")

    await pool.close()


if __name__ == "__main__":
    asyncio.run(rotate())
