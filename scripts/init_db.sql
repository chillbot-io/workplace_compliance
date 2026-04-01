-- scripts/init_db.sql — run as postgres superuser on first setup
-- Usage: sudo -u postgres psql -f scripts/init_db.sql
--
-- Creates the stablelabel database and 3 least-privilege roles.
-- Run ONCE on the API server before first deploy.

-- 1. Create database
CREATE DATABASE stablelabel;
\c stablelabel

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 2. API user — owns all tables, used by FastAPI via pgBouncer
CREATE ROLE api WITH LOGIN PASSWORD 'CHANGE_ME_API' NOSUPERUSER NOCREATEDB NOCREATEROLE;
GRANT ALL PRIVILEGES ON DATABASE stablelabel TO api;

-- 3. Pipeline user — remote, writes to employer_profile + related tables only
CREATE ROLE pipeline_user WITH LOGIN PASSWORD 'CHANGE_ME_PIPELINE' NOSUPERUSER NOCREATEDB NOCREATEROLE;
GRANT CONNECT ON DATABASE stablelabel TO pipeline_user;

-- 4. Metabase user — read-only on all tables (for dashboards)
CREATE ROLE metabase_user WITH LOGIN PASSWORD 'CHANGE_ME_METABASE' NOSUPERUSER NOCREATEDB NOCREATEROLE;
GRANT CONNECT ON DATABASE stablelabel TO metabase_user;

-- NOTE: Table-level grants are applied AFTER migrations run (see migrations/006_grants.sql).
-- At this point no tables exist yet, so we only grant database-level access.
