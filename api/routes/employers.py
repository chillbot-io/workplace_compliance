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
                await record_usage(key_row, "/v1/employers")
                headers = await get_quota_headers(key_row)
                return JSONResponse(
                    content=_format_results(rows, count, limit, offset),
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

            # Use lower threshold for short names (< 6 chars get penalized by pg_trgm)
            sim_threshold = 0.1 if len(name) < 6 else 0.15

            # Count total matches
            count = await con.fetchval(f"""
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT ON (employer_id) employer_id
                    FROM employer_profile
                    WHERE similarity(employer_name, $1) > {sim_threshold}{extra_where}
                    ORDER BY employer_id, snapshot_date DESC
                ) sub
            """, *params)

            # Fetch page of results sorted by risk_score desc
            rows = await con.fetch(f"""
                SELECT * FROM (
                    SELECT DISTINCT ON (employer_id) *,
                           similarity(employer_name, $1) AS sim_score
                    FROM employer_profile
                    WHERE similarity(employer_name, $1) > {sim_threshold}{extra_where}
                    ORDER BY employer_id, snapshot_date DESC
                ) sub
                ORDER BY risk_score DESC NULLS LAST
                LIMIT ${param_idx} OFFSET ${param_idx + 1}
            """, *params, limit, offset)

            if not rows:
                # Don't charge quota for zero-result searches
                return JSONResponse(
                    status_code=404,
                    content={
                        "error": "no_results",
                        "message": f'No employers found matching "{name}".',
                        "suggestions": [
                            "Try a shorter or simpler name (e.g., 'walmart' instead of 'walmart inc')",
                            "Check spelling",
                            "Remove zip/state filters to broaden the search",
                        ],
                    },
                )

            await record_usage(key_row, "/v1/employers")
            headers = await get_quota_headers(key_row)
            return JSONResponse(
                content=_format_results(rows, count, limit, offset),
                headers=headers,
            )

        raise HTTPException(400, detail={
            "error": "missing_query",
            "message": "Provide at least one of: name or ein.",
        })


## --- Parent Company Rollup ---

@router.get("/employers/parent")
async def get_parent_company(
    key_row=Depends(check_scope("employer:read")),
    name: str = Query(..., description="Parent company name (e.g., 'Amazon', 'Walmart')"),
    state: str | None = Query(None, description="Filter locations by state"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """Parent company risk rollup — aggregate risk across all locations.

    Returns company-wide statistics (total inspections, violations, penalties,
    risk distribution) plus a paginated list of individual locations sorted
    by risk score (worst first).

    Premium feature for enterprise customers evaluating national employers.
    """
    await check_monthly_quota(key_row)
    parent_name = name  # rename for clarity in queries

    async with get_pool().acquire() as con:
        # Build filter
        params = [parent_name]
        param_idx = 2
        extra_where = ""

        if state:
            extra_where = f" AND state = ${param_idx}"
            params.append(state.upper())
            param_idx += 1

        # Check parent exists (exact match first)
        total_locations = await con.fetchval(f"""
            SELECT COUNT(DISTINCT employer_id)
            FROM employer_profile
            WHERE parent_name = $1{extra_where}
              AND snapshot_date = (SELECT MAX(snapshot_date) FROM employer_profile)
        """, *params)

        if not total_locations:
            # Fall back to fuzzy name match
            total_locations = await con.fetchval(f"""
                SELECT COUNT(DISTINCT employer_id)
                FROM employer_profile
                WHERE similarity(parent_name, $1) > 0.5{extra_where}
                  AND snapshot_date = (SELECT MAX(snapshot_date) FROM employer_profile)
            """, *params)

            if not total_locations:
                raise HTTPException(404, detail={
                    "error": "parent_not_found",
                    "message": f'No parent company found matching "{parent_name}".',
                })

            name_filter = "similarity(parent_name, $1) > 0.5"
        else:
            name_filter = "parent_name = $1"

        # Aggregate stats across all locations
        agg = await con.fetchrow(f"""
            SELECT
                COUNT(DISTINCT employer_id) AS total_locations,
                SUM(osha_inspections_5yr) AS total_inspections_5yr,
                SUM(osha_violations_5yr) AS total_violations_5yr,
                SUM(osha_total_penalties) AS total_penalties_5yr,
                AVG(risk_score) AS avg_risk_score,
                MAX(risk_score) AS max_risk_score,
                MIN(risk_score) AS min_risk_score,
                SUM(CASE WHEN risk_tier = 'HIGH' THEN 1 ELSE 0 END) AS high_count,
                SUM(CASE WHEN risk_tier = 'ELEVATED' THEN 1 ELSE 0 END) AS elevated_count,
                SUM(CASE WHEN risk_tier = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_count,
                SUM(CASE WHEN risk_tier = 'LOW' THEN 1 ELSE 0 END) AS low_count,
                SUM(CASE WHEN trend_signal = 'WORSENING' THEN 1 ELSE 0 END) AS worsening_count,
                SUM(CASE WHEN trend_signal = 'IMPROVING' THEN 1 ELSE 0 END) AS improving_count
            FROM (
                SELECT DISTINCT ON (employer_id) *
                FROM employer_profile
                WHERE {name_filter}{extra_where}
                ORDER BY employer_id, snapshot_date DESC
            ) sub
        """, *params)

        # States breakdown
        states = await con.fetch(f"""
            SELECT state, COUNT(DISTINCT employer_id) AS location_count
            FROM (
                SELECT DISTINCT ON (employer_id) employer_id, state
                FROM employer_profile
                WHERE {name_filter}{extra_where}
                ORDER BY employer_id, snapshot_date DESC
            ) sub
            WHERE state IS NOT NULL
            GROUP BY state
            ORDER BY location_count DESC
        """, *params)

        # Paginated locations sorted by risk score (worst first)
        locations = await con.fetch(f"""
            SELECT * FROM (
                SELECT DISTINCT ON (employer_id) *
                FROM employer_profile
                WHERE {name_filter}{extra_where}
                ORDER BY employer_id, snapshot_date DESC
            ) sub
            ORDER BY risk_score DESC NULLS LAST
            LIMIT ${param_idx} OFFSET ${param_idx + 1}
        """, *params, limit, offset)

    await record_usage(key_row, "/v1/employers/parent/{{name}}")
    headers = await get_quota_headers(key_row)

    agg_dict = dict(agg) if agg else {}

    return JSONResponse(
        content=json.loads(json.dumps({
            "parent_name": parent_name,
            "total_locations": agg_dict.get("total_locations", 0),
            "aggregate": {
                "total_inspections_5yr": agg_dict.get("total_inspections_5yr", 0),
                "total_violations_5yr": agg_dict.get("total_violations_5yr", 0),
                "total_penalties_5yr": agg_dict.get("total_penalties_5yr", 0),
                "avg_risk_score": round(agg_dict.get("avg_risk_score") or 0, 1),
                "max_risk_score": agg_dict.get("max_risk_score", 0),
                "min_risk_score": agg_dict.get("min_risk_score", 0),
                "risk_distribution": {
                    "HIGH": agg_dict.get("high_count", 0),
                    "ELEVATED": agg_dict.get("elevated_count", 0),
                    "MEDIUM": agg_dict.get("medium_count", 0),
                    "LOW": agg_dict.get("low_count", 0),
                },
                "trend_distribution": {
                    "WORSENING": agg_dict.get("worsening_count", 0),
                    "IMPROVING": agg_dict.get("improving_count", 0),
                    "STABLE": (agg_dict.get("total_locations", 0)
                               - agg_dict.get("worsening_count", 0)
                               - agg_dict.get("improving_count", 0)),
                },
            },
            "states": [
                {"state": dict(s)["state"], "location_count": dict(s)["location_count"]}
                for s in states
            ],
            "locations": {
                "results": [_format_employer(r) for r in locations],
                "total_count": agg_dict.get("total_locations", 0),
                "limit": limit,
                "offset": offset,
            },
        }, cls=CustomEncoder)),
        headers=headers,
    )


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
    for key in ["sim_score", "created_at", "updated_at", "pipeline_run_id", "name_normalized"]:
        r.pop(key, None)

    # Roundtrip through custom encoder to handle Decimal, UUID, dates
    result = json.loads(json.dumps(r, cls=CustomEncoder))

    # Add context for risk scores
    inspections = result.get("osha_inspections_5yr", 0) or 0
    risk_score = result.get("risk_score", 0) or 0
    if inspections == 0 and risk_score == 0:
        result["risk_note"] = "No OSHA inspections in the last 5 years. This does not mean the employer is violation-free — OSHA inspects a small fraction of workplaces annually."

    return result


## --- Batch Lookup ---

class BatchLookupItem(BaseModel):
    name: str | None = None
    ein: str | None = None
    employer_id: str | None = None
    state: str | None = None
    zip: str | None = None
    city: str | None = None

    class Config:
        str_max_length = 200


class BatchLookupRequest(BaseModel):
    lookups: list[BatchLookupItem]


BATCH_MAX = 500
BATCH_SYNC_LIMIT = 100


@router.post("/employers/batch")
async def batch_lookup(
    body: BatchLookupRequest,
    key_row=Depends(check_scope("batch:write")),
):
    """Batch employer lookup — designed for spreadsheet-style bulk searches.

    Each item can include name, ein, employer_id, plus optional location
    filters (state, zip, city) to narrow to a specific site.

    Up to 100 items processed synchronously. 101-500 returns a job_id
    for async polling (coming soon). Max 500 items per request.

    Typical usage: underwriter uploads a spreadsheet with company name +
    address for each policy they're quoting.
    """
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
        raise HTTPException(422, detail={
            "error": "batch_too_large_for_sync",
            "message": f"Batches over {BATCH_SYNC_LIMIT} items are not yet supported. Split your request into chunks of {BATCH_SYNC_LIMIT} or fewer.",
            "your_count": len(body.lookups),
            "max_sync": BATCH_SYNC_LIMIT,
            "tip": "For larger batches, use POST /v1/employers/upload-csv which supports up to 500 rows.",
        })

    results = []
    async with get_pool().acquire() as con:
        for item in body.lookups:
            match = await _resolve_batch_item(item, con)
            results.append({
                "query": item.model_dump(exclude_none=True),
                "match": _format_employer(match) if match else None,
                "confidence": _match_confidence(match) if match else None,
            })

    await record_usage(key_row, "/v1/employers/batch", count=len(body.lookups))
    headers = await get_quota_headers(key_row)

    return JSONResponse(
        content={
            "results": results,
            "total": len(results),
            "matched": sum(1 for r in results if r["match"]),
            "unmatched": sum(1 for r in results if not r["match"]),
        },
        headers=headers,
    )


async def _resolve_batch_item(item: BatchLookupItem, con) -> dict | None:
    """Resolve a single batch item to the best employer match.

    Priority: employer_id (exact) > ein (exact) > name (fuzzy + location filters).
    Location filters (state, zip, city) narrow name matches to a specific site.
    """
    # Direct ID lookup — no ambiguity
    if item.employer_id:
        return await con.fetchrow("""
            SELECT DISTINCT ON (employer_id) *
            FROM employer_profile
            WHERE employer_id = $1::uuid
            ORDER BY employer_id, snapshot_date DESC
        """, item.employer_id)

    # EIN exact match — may have multiple locations, use location to pick best
    if item.ein:
        params = [item.ein]
        param_idx = 2
        filters = []

        if item.state:
            filters.append(f"state = ${param_idx}")
            params.append(item.state.upper().strip())
            param_idx += 1
        if item.zip:
            filters.append(f"zip = ${param_idx}")
            params.append(item.zip.strip()[:5])
            param_idx += 1

        extra = (" AND " + " AND ".join(filters)) if filters else ""

        return await con.fetchrow(f"""
            SELECT * FROM (
                SELECT DISTINCT ON (employer_id) *
                FROM employer_profile
                WHERE ein = $1{extra}
                ORDER BY employer_id, snapshot_date DESC
            ) sub
            ORDER BY risk_score DESC NULLS LAST
            LIMIT 1
        """, *params)

    # Name fuzzy match with location narrowing
    if item.name:
        params = [item.name]
        param_idx = 2
        filters = []

        if item.state:
            filters.append(f"state = ${param_idx}")
            params.append(item.state.upper().strip())
            param_idx += 1
        if item.zip:
            filters.append(f"zip = ${param_idx}")
            params.append(item.zip.strip()[:5])
            param_idx += 1
        if item.city:
            # Use similarity for city to handle spelling variations
            filters.append(f"similarity(city, ${param_idx}) > 0.4")
            params.append(item.city.upper().strip())
            param_idx += 1

        extra = (" AND " + " AND ".join(filters)) if filters else ""

        # When location filters are present, use a tighter name threshold
        # and rank by combined name similarity + location match
        name_threshold = 0.3 if filters else 0.3

        return await con.fetchrow(f"""
            SELECT * FROM (
                SELECT DISTINCT ON (employer_id) *,
                       similarity(employer_name, $1) AS sim_score
                FROM employer_profile
                WHERE similarity(employer_name, $1) > {name_threshold}{extra}
                ORDER BY employer_id, snapshot_date DESC
            ) sub
            ORDER BY sim_score DESC
            LIMIT 1
        """, *params)

    return None


def _match_confidence(row) -> str:
    """Estimate match confidence based on how the match was found."""
    if not row:
        return None
    sim = row.get("sim_score")
    if sim is None:
        return "exact"  # ID or EIN match
    if sim > 0.8:
        return "high"
    if sim > 0.5:
        return "medium"
    return "low"


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

def _format_results(rows, total_count: int, limit: int, offset: int) -> dict:
    """Format search results as a flat paginated list.
    location_count is pre-computed in the pipeline (gold model)."""
    return {
        "results": [_format_employer(r) for r in rows],
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "data_notes": {
            "freshness": "OSHA citations typically appear 3-8 months after inspection date. WHD data updates monthly.",
            "coverage": "Data includes OSHA inspections/violations and WHD wage enforcement actions since FY2005.",
            "scoring": "Risk scores combine OSHA violation severity (willful, repeat, serious) and WHD back wages. See /v1/health for data age.",
        },
    }
