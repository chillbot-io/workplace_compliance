# FastDOL Pre-Launch Checklist

## Pipeline Server (46.224.150.38)

- [ ] WHD ingestion complete (~300k+ records)
- [ ] Full pipeline re-run:
  ```bash
  cd /opt/employer-compliance && git pull && source venv/bin/activate && set -a && source .env.pipeline && set +a
  python pipeline/load_bronze.py
  python pipeline/load_parent_companies.py
  python pipeline/update_naics_seed.py
  cd dbt && dbt seed --exclude parent_companies && dbt run --select staging silver && cd ..
  python pipeline/parse_addresses.py
  python pipeline/entity_resolution.py
  cd dbt && dbt run --select gold && cd ..
  python pipeline/validate_data.py
  python pipeline/sync.py
  python pipeline/validate_sync.py
  ```
- [ ] Install crontab:
  ```bash
  crontab /opt/employer-compliance/scripts/crontab.pipeline && crontab -l
  ```
- [ ] Verify nightly pipeline runs successfully (check next morning)

## API Server (88.198.218.234)

- [ ] Pull new code and install dependency:
  ```bash
  cd /opt/employer-compliance && git fetch origin && git checkout claude/project-onboarding-3HzZZ && git pull
  source venv/bin/activate && pip install python-multipart
  ```
- [ ] Run migration 004 (new columns + indexes):
  ```bash
  psql "$DATABASE_URL" -f migrations/004_search_indexes.sql
  ```
- [ ] Restart API:
  ```bash
  sudo systemctl restart fastdol-api && sudo systemctl status fastdol-api
  ```
- [ ] Verify health:
  ```bash
  curl -s https://api.fastdol.com/v1/health | python3 -m json.tool
  ```

## Security (BEFORE public launch)

- [ ] Change Postgres passwords from defaults (password1/2/3):
  ```sql
  -- On API server Postgres:
  ALTER USER api PASSWORD 'new-strong-password-here';
  ALTER USER pipeline_user PASSWORD 'new-strong-password-here';
  ALTER USER metabase_user PASSWORD 'new-strong-password-here';
  ```
  Then update `.env.api` and `.env.pipeline` on both servers.

- [ ] Generate new JWT RSA keys (if using defaults):
  ```bash
  openssl genrsa -out /etc/employer-compliance/jwt_private.pem 2048
  openssl rsa -in /etc/employer-compliance/jwt_private.pem -pubout -out /etc/employer-compliance/jwt_public.pem
  ```

- [ ] Generate CSRF secret:
  ```bash
  python3 -c "import secrets; print(secrets.token_hex(32))"
  ```
  Add to `.env.api` as `CSRF_SECRET=...`

## Stripe

- [ ] Switch from test mode to live mode in Stripe dashboard
- [ ] Update `.env.api`:
  - `STRIPE_SECRET_KEY=sk_live_...`
  - `STRIPE_WEBHOOK_SECRET=whsec_...`
- [ ] Create live webhook endpoint in Stripe pointing to `https://api.fastdol.com/webhooks/stripe`
- [ ] Create live price IDs for each plan tier and update billing route if hardcoded

## Monitoring

- [ ] Set up Sentry project, add DSN to `.env.api` as `SENTRY_DSN=...`
- [ ] Set up UptimeRobot (or similar) on `https://api.fastdol.com/v1/health`
- [ ] Set up Slack webhook for pipeline alerts, add to `.env.pipeline` as `ALERT_WEBHOOK_URL=...`

## Validation

- [ ] Run ground truth validation:
  ```bash
  python pipeline/validate_ground_truth.py
  ```
  Spot-check 10+ employers against https://www.osha.gov/ords/imis/establishment.html

- [ ] Run entity resolution validation:
  ```bash
  python pipeline/validate_entity_resolution.py
  ```

- [ ] Test full API flow: signup → verify email → login → create key → search → batch → CSV upload

## Post-Launch

- [ ] Monitor first nightly pipeline run
- [ ] Monitor first weekly WHD run (Sunday)
- [ ] Monitor first monthly subsidiary refresh (1st of month)
- [ ] Check DQ snapshots at `/data/dq_snapshots/` for trends
- [ ] Review feedback submissions at `/v1/employers/{id}/feedback`
