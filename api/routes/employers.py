"""
api/routes/employers.py — Employer search, direct lookup, inspections, batch, risk history, and feedback.
"""

import json
from decimal import Decimal
from uuid import UUID

from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from api.auth import check_scope, check_monthly_quota, record_usage, get_quota_headers, get_pool


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, UUID):
            return str(obj)
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        return super().default(obj)

router = APIRouter(prefix="/v1")


@router.get("/employers")
async def search_employers(
    key_row=Depends(check_scope("employer:read")),
    name: str | None = Query(None, description="Fuzzy employer name search"),
    ein: str | None = Query(None, description="Exact EIN match"),
    state: str | None = Query(None, description="State code filter (e.g., CA)"),
    zip: str | None = Query(None, description="5-digit zip code filter"),
    naics: str | None = Query(None, description="4-digit NAICS prefix filter"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """Search employers by name, EIN, zip, state, or NAICS.

    Returns a flat list of employer locations sorted by risk score (worst first).
    Each result includes `related_locations_count` showing how many locations
    share the same normalized employer name — useful for national chains.
    """
    if not any([name, ein]):
        raise HTTPException(400, detail={
            "error": "missing_query",
            "message": "Provide at least one of: name or ein.",
        })

    await check_monthly_quota(key_row)

    async with get_pool().acquire() as con:
        # EIN exact match — highest priority, skip fuzzy
        if ein:
            params = [ein]
            param_idx = 2
            where_clauses = ["ein = $1"]

            if state:
                where_clauses.append(f"state = ${param_idx}")
                params.append(state.upper())
                param_idx += 1
            if zip:
                where_clauses.append(f"zip = ${param_idx}")
                params.append(zip.strip()[:5])
                param_idx += 1

            where_sql = " AND ".join(where_clauses)

            count = await con.fetchval(f"""
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT ON (employer_id) employer_id
                    FROM employer_profile
                    WHERE {where_sql}
                    ORDER BY employer_id, snapshot_date DESC
                ) sub
            """, *params)

            rows = await con.fetch(f"""
                SELECT * FROM (
                    SELECT DISTINCT ON (employer_id) *
                    FROM employer_profile
                    WHERE {where_sql}
                    ORDER BY employer_id, snapshot_date DESC
                ) sub
                ORDER BY risk_score DESC NULLS LAST
                LIMIT ${param_idx} OFFSET ${param_idx + 1}
            """, *params, limit, offset)

            if rows:
                loc_counts = await _add_location_counts(rows, con)
                await record_usage(key_row, "/v1/employers")
                headers = await get_quota_headers(key_row)
                return JSONResponse(
                    content=_format_results(rows, count, limit, offset, loc_counts),
                    headers=headers,
                )

        # Name fuzzy search with pg_trgm + optional filters
        if name:
            params = [name]
            param_idx = 2
            filter_clauses = []

            if state:
                filter_clauses.append(f"state = ${param_idx}")
                params.append(state.upper())
                param_idx += 1
            if zip:
                filter_clauses.append(f"zip = ${param_idx}")
                params.append(zip.strip()[:5])
                param_idx += 1
            if naics:
                filter_clauses.append(f"naics_code LIKE ${param_idx}")
                params.append(f"{naics}%")
                param_idx += 1

            extra_where = (" AND " + " AND ".join(filter_clauses)) if filter_clauses else ""

            # Count total matches
            count = await con.fetchval(f"""
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT ON (employer_id) employer_id
                    FROM employer_profile
                    WHERE similarity(employer_name, $1) > 0.2{extra_where}
                    ORDER BY employer_id, snapshot_date DESC
                ) sub
            """, *params)

            # Fetch page of results sorted by risk_score desc
            rows = await con.fetch(f"""
                SELECT * FROM (
                    SELECT DISTINCT ON (employer_id) *,
                           similarity(employer_name, $1) AS sim_score
                    FROM employer_profile
                    WHERE similarity(employer_name, $1) > 0.2{extra_where}
                    ORDER BY employer_id, snapshot_date DESC
                ) sub
                ORDER BY risk_score DESC NULLS LAST
                LIMIT ${param_idx} OFFSET ${param_idx + 1}
            """, *params, limit, offset)

            if not rows:
                raise HTTPException(404, detail={
                    "error": "no_results",
                    "message": f'No employers found matching "{name}".',
                })

            loc_counts = await _add_location_counts(rows, con)
            await record_usage(key_row, "/v1/employers")
            headers = await get_quota_headers(key_row)
            return JSONResponse(
                content=_format_results(rows, count, limit, offset, loc_counts),
                headers=headers,
            )

        raise HTTPException(400, detail={
            "error": "missing_query",
            "message": "Provide at least one of: name or ein.",
        })


@router.get("/employers/{employer_id}")
async def get_employer(
    employer_id: str,
    key_row=Depends(check_scope("employer:read")),
):
    """Direct lookup by employer_id UUID."""
    await check_monthly_quota(key_row)

    async with get_pool().acquire() as con:
        # Check for superseded employer_id (redirect)
        superseded = await con.fetchval("""
            SELECT superseded_by FROM cluster_id_mapping
            WHERE employer_id = $1::uuid AND superseded_by IS NOT NULL
            LIMIT 1
        """, employer_id)

        if superseded:
            return JSONResponse(
                status_code=301,
                headers={"Location": f"/v1/employers/{superseded}"},
                content={
                    "error": "employer_moved",
                    "message": f"This employer ID has been superseded. Redirect to /v1/employers/{superseded}",
                },
            )

        row = await con.fetchrow("""
            SELECT DISTINCT ON (employer_id) *
            FROM employer_profile
            WHERE employer_id = $1::uuid
            ORDER BY employer_id, snapshot_date DESC
        """, employer_id)

    if not row:
        raise HTTPException(404, detail={
            "error": "employer_not_found",
            "message": f"No employer found with ID {employer_id}.",
        })

    await record_usage(key_row, "/v1/employers/{id}")
    headers = await get_quota_headers(key_row)
    return JSONResponse(
        content={"match": _format_employer(row)},
        headers=headers,
    )


@router.get("/employers/{employer_id}/inspections")
async def get_inspections(
    employer_id: str,
    key_row=Depends(check_scope("employer:read")),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """Get inspection history for an employer. Free — not metered."""
    async with get_pool().acquire() as con:
        rows = await con.fetch("""
            SELECT * FROM inspection_history
            WHERE employer_id = $1::uuid
            ORDER BY inspection_date DESC
            LIMIT $2 OFFSET $3
        """, employer_id, limit, offset)

        total = await con.fetchval("""
            SELECT COUNT(*) FROM inspection_history
            WHERE employer_id = $1::uuid
        """, employer_id)

    return JSONResponse(
        content={
            "data": [json.loads(json.dumps(dict(r), cls=CustomEncoder)) for r in rows],
            "pagination": {
                "total": total,
                "limit": limit,
                "offset": offset,
            },
        },
        headers={"X-Billing-Note": "not-metered"},
    )


def _format_employer(row) -> dict:
    """Format a database row into the API response shape."""
    r = dict(row)
    # Remove internal fields
    for key in ["sim_score", "created_at", "updated_at", "pipeline_run_id"]:
        r.pop(key, None)

    # Roundtrip through custom encoder to handle Decimal, UUID, dates
    return json.loads(json.dumps(r, cls=CustomEncoder))


## --- Batch Lookup ---

class BatchLookupItem(BaseModel):
    name: str | None = None
    ein: str | None = None
    employer_id: str | None = None


class BatchLookupRequest(BaseModel):
    lookups: list[BatchLookupItem]


BATCH_MAX = 500
BATCH_SYNC_LIMIT = 25


@router.post("/employers/batch")
async def batch_lookup(
    body: BatchLookupRequest,
    key_row=Depends(check_scope("batch:write")),
):
    """Batch employer lookup. <=25 sync, >25 async (not yet implemented), max 500."""
    if len(body.lookups) > BATCH_MAX:
        raise HTTPException(422, detail={
            "error": "batch_too_large",
            "message": f"Maximum {BATCH_MAX} items per batch. Got {len(body.lookups)}.",
        })

    if not body.lookups:
        raise HTTPException(400, detail={
            "error": "empty_batch",
            "message": "Provide at least one lookup item.",
        })

    await check_monthly_quota(key_row)

    if len(body.lookups) > BATCH_SYNC_LIMIT:
        # TODO: async batch with R2 storage and job polling
        raise HTTPException(501, detail={
            "error": "async_not_implemented",
            "message": f"Batches over {BATCH_SYNC_LIMIT} items require async processing (coming soon). Use {BATCH_SYNC_LIMIT} or fewer for synchronous results.",
        })

    results = []
    async with get_pool().acquire() as con:
        for item in body.lookups:
            row = None

            # Priority: employer_id > ein > name
            if item.employer_id:
                row = await con.fetchrow("""
                    SELECT DISTINCT ON (employer_id) *
                    FROM employer_profile
                    WHERE employer_id = $1::uuid
                    ORDER BY employer_id, snapshot_date DESC
                """, item.employer_id)

            elif item.ein:
                row = await con.fetchrow("""
                    SELECT DISTINCT ON (employer_id) *
                    FROM employer_profile
                    WHERE ein = $1
                    ORDER BY employer_id, snapshot_date DESC
                """, item.ein)

            elif item.name:
                row = await con.fetchrow("""
                    SELECT * FROM (
                        SELECT DISTINCT ON (employer_id) *,
                               similarity(employer_name, $1) AS sim_score
                        FROM employer_profile
                        WHERE similarity(employer_name, $1) > 0.3
                        ORDER BY employer_id, snapshot_date DESC
                    ) sub
                    ORDER BY sim_score DESC
                    LIMIT 1
                """, item.name)

            if row:
                results.append({
                    "query": item.model_dump(exclude_none=True),
                    "match": _format_employer(row),
                })
            else:
                results.append({
                    "query": item.model_dump(exclude_none=True),
                    "match": None,
                })

    await record_usage(key_row, "/v1/employers/batch", count=len(body.lookups))
    headers = await get_quota_headers(key_row)

    return JSONResponse(
        content={
            "data": results,
            "total": len(results),
            "matched": sum(1 for r in results if r["match"]),
        },
        headers=headers,
    )


## --- Risk History ---

@router.get("/employers/{employer_id}/risk-history")
async def get_risk_history(
    employer_id: str,
    key_row=Depends(check_scope("employer:read")),
    limit: int = Query(90, ge=1, le=365),
):
    """Risk tier history for an employer over time. Shows nightly snapshots."""
    await check_monthly_quota(key_row)

    async with get_pool().acquire() as con:
        rows = await con.fetch("""
            SELECT snapshot_date, risk_tier, risk_score, confidence_tier, trend_signal
            FROM risk_snapshots
            WHERE employer_id = $1::uuid
            ORDER BY snapshot_date DESC
            LIMIT $2
        """, employer_id, limit)

        if not rows:
            # Fall back to employer_profile snapshots if risk_snapshots not populated yet
            rows = await con.fetch("""
                SELECT snapshot_date, risk_tier, risk_score, confidence_tier, trend_signal
                FROM employer_profile
                WHERE employer_id = $1::uuid
                ORDER BY snapshot_date DESC
                LIMIT $2
            """, employer_id, limit)

    if not rows:
        raise HTTPException(404, detail={
            "error": "no_history",
            "message": f"No risk history found for employer {employer_id}.",
        })

    await record_usage(key_row, "/v1/employers/{id}/risk-history")
    headers = await get_quota_headers(key_row)

    return JSONResponse(
        content={
            "employer_id": employer_id,
            "data": [json.loads(json.dumps(dict(r), cls=CustomEncoder)) for r in rows],
            "total": len(rows),
        },
        headers=headers,
    )


## --- Feedback ---

class FeedbackRequest(BaseModel):
    type: str
    description: str | None = None
    contact_email: str | None = None

    class Config:
        str_max_length = 5000


@router.post("/employers/{employer_id}/feedback")
async def submit_feedback(
    employer_id: str,
    body: FeedbackRequest,
    key_row=Depends(check_scope("employer:read")),
):
    """Submit feedback about an employer match. Not metered."""
    valid_types = ("incorrect_match", "missing_data", "wrong_employer", "other")
    if body.type not in valid_types:
        raise HTTPException(400, detail={
            "error": "invalid_feedback_type",
            "message": f"Type must be one of: {', '.join(valid_types)}",
        })

    async with get_pool().acquire() as con:
        await con.execute("""
            INSERT INTO feedback (employer_id, customer_id, type, description, contact_email)
            VALUES ($1::uuid, $2, $3, $4, $5)
        """, employer_id, key_row.get("customer_id"), body.type, body.description, body.contact_email)

    return JSONResponse(
        content={"status": "received", "message": "Thank you for your feedback."},
        headers={"X-Billing-Note": "not-metered"},
    )


## --- Industry Benchmarks ---

@router.get("/industries/{naics4}")
async def get_industry(
    naics4: str,
    key_row=Depends(check_scope("employer:read")),
):
    """Industry-level risk benchmarks for a 4-digit NAICS code."""
    if len(naics4) != 4 or not naics4.isdigit():
        raise HTTPException(400, detail={
            "error": "invalid_naics",
            "message": "Provide a 4-digit NAICS code.",
        })

    async with get_pool().acquire() as con:
        row = await con.fetchrow("""
            SELECT
                LEFT(naics_code, 4) AS naics_4digit,
                COUNT(DISTINCT employer_id) AS employer_count,
                AVG(osha_inspections_5yr) AS avg_inspections_5yr,
                AVG(osha_violations_5yr) AS avg_violations_5yr,
                AVG(osha_total_penalties) AS avg_penalties_5yr,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY osha_total_penalties) AS median_penalties_5yr,
                SUM(CASE WHEN risk_tier = 'HIGH' THEN 1 ELSE 0 END) AS high_count,
                SUM(CASE WHEN risk_tier = 'ELEVATED' THEN 1 ELSE 0 END) AS elevated_count,
                SUM(CASE WHEN risk_tier = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_count,
                SUM(CASE WHEN risk_tier = 'LOW' THEN 1 ELSE 0 END) AS low_count
            FROM employer_profile
            WHERE LEFT(naics_code, 4) = $1
              AND snapshot_date = (SELECT MAX(snapshot_date) FROM employer_profile)
            GROUP BY LEFT(naics_code, 4)
        """, naics4)

    if not row:
        raise HTTPException(404, detail={
            "error": "naics_not_found",
            "message": f"No data for NAICS {naics4}.",
        })

    return JSONResponse(
        content=json.loads(json.dumps(dict(row), cls=CustomEncoder)),
        headers={"X-Billing-Note": "not-metered"},
    )


@router.get("/industries/naics-codes")
async def list_naics_codes(
    key_row=Depends(check_scope("employer:read")),
):
    """List all 4-digit NAICS codes with employer counts."""
    async with get_pool().acquire() as con:
        rows = await con.fetch("""
            SELECT LEFT(naics_code, 4) AS naics_4digit,
                   COUNT(DISTINCT employer_id) AS employer_count
            FROM employer_profile
            WHERE naics_code IS NOT NULL
              AND snapshot_date = (SELECT MAX(snapshot_date) FROM employer_profile)
            GROUP BY LEFT(naics_code, 4)
            ORDER BY employer_count DESC
        """)

    return JSONResponse(
        content={
            "data": [json.loads(json.dumps(dict(r), cls=CustomEncoder)) for r in rows],
            "total": len(rows),
        },
        headers={"X-Billing-Note": "not-metered"},
    )


## --- Helpers ---

async def _add_location_counts(rows, con) -> dict[str, int]:
    """Get related_locations_count for each employer name in the result set.
    Counts how many distinct employer_ids share a similar normalized name."""
    if not rows:
        return {}

    # Collect unique employer names from results
    names = list({r["employer_name"] for r in rows if r.get("employer_name")})
    if not names:
        return {}

    # Batch count: for each name, count employers with similarity > 0.6
    # (tighter than search threshold — these are "same company" matches)
    counts = {}
    for name in names:
        count = await con.fetchval("""
            SELECT COUNT(DISTINCT employer_id)
            FROM employer_profile
            WHERE similarity(employer_name, $1) > 0.6
              AND snapshot_date = (SELECT MAX(snapshot_date) FROM employer_profile)
        """, name)
        counts[name] = count or 0

    return counts


def _format_results(rows, total_count: int, limit: int, offset: int, loc_counts: dict | None = None) -> dict:
    """Format search results as a flat paginated list."""
    results = []
    for r in rows:
        emp = _format_employer(r)
        if loc_counts:
            emp["related_locations_count"] = loc_counts.get(r["employer_name"], 0)
        results.append(emp)

    return {
        "results": results,
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
    }
