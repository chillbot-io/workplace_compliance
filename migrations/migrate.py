"""
migrations/migrate.py — Sequential migration runner.
Runs at container startup before the app. Idempotent — skips already-applied migrations.
Usage: DATABASE_URL=postgresql://... python migrations/migrate.py
"""

import os
import re
import psycopg2


def migrate():
    database_url = os.environ.get("DATABASE_URL") or os.environ.get("PG_DSN")
    if not database_url:
        raise RuntimeError("Set DATABASE_URL or PG_DSN environment variable")

    con = psycopg2.connect(database_url)
    con.autocommit = False
    cur = con.cursor()

    # Ensure tracking table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version     INTEGER PRIMARY KEY,
            filename    TEXT NOT NULL,
            applied_at  TIMESTAMP DEFAULT NOW()
        )
    """)
    con.commit()

    # Get already-applied versions
    cur.execute("SELECT version FROM schema_migrations ORDER BY version")
    applied = {row[0] for row in cur.fetchall()}

    # Find migration files
    migration_dir = os.path.dirname(os.path.abspath(__file__))
    files = sorted(f for f in os.listdir(migration_dir) if re.match(r"^\d{3}_.*\.sql$", f))

    applied_count = 0
    for f in files:
        version = int(f[:3])
        if version in applied:
            continue
        print(f"Applying migration {f}...")
        sql = open(os.path.join(migration_dir, f)).read()
        try:
            cur.execute(sql)
            cur.execute(
                "INSERT INTO schema_migrations (version, filename) VALUES (%s, %s)",
                (version, f),
            )
            con.commit()
            applied_count += 1
        except Exception:
            con.rollback()
            raise

    if applied_count == 0:
        print("All migrations already applied.")
    else:
        print(f"Applied {applied_count} migration(s).")

    cur.close()
    con.close()


if __name__ == "__main__":
    migrate()
