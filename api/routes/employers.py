"""
api/routes/employers.py — Employer search, direct lookup, and inspections endpoints.
"""

import json
from decimal import Decimal
from uuid import UUID

from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi.responses import JSONResponse

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
    address: str | None = Query(None, description="Free-text address"),
    state: str | None = Query(None, description="State code filter (e.g., CA)"),
    naics: str | None = Query(None, description="4-digit NAICS filter"),
    limit: int = Query(10, ge=1, le=50),
):
    """Search employers by name, EIN, address, state, or NAICS."""
    if not any([name, ein, address]):
        raise HTTPException(400, detail={
            "error": "missing_query",
            "message": "Provide at least one of: name, ein, or address.",
        })

    await check_monthly_quota(key_row)

    async with get_pool().acquire() as con:
        # EIN exact match — highest priority
        if ein:
            rows = await con.fetch("""
                SELECT DISTINCT ON (employer_id) *
                FROM employer_profile
                WHERE ein = $1
                ORDER BY employer_id, snapshot_date DESC
                LIMIT $2
            """, ein, limit)

            if rows:
                await record_usage(key_row, "/v1/employers")
                headers = await get_quota_headers(key_row)
                return JSONResponse(
                    content=_format_search_response(rows),
                    headers=headers,
                )

        # Name fuzzy search with pg_trgm
        if name:
            query = """
                SELECT DISTINCT ON (employer_id) *,
                       similarity(employer_name, $1) AS sim_score
                FROM employer_profile
                WHERE similarity(employer_name, $1) > 0.2
            """
            params = [name]
            param_idx = 2

            if state:
                query += f" AND state = ${param_idx}"
                params.append(state.upper())
                param_idx += 1

            if naics:
                query += f" AND naics_code LIKE ${param_idx}"
                params.append(f"{naics}%")
                param_idx += 1

            query += f"""
                ORDER BY employer_id, snapshot_date DESC
            """

            # Wrap to sort by similarity
            query = f"""
                SELECT * FROM ({query}) sub
                ORDER BY sim_score DESC
                LIMIT ${param_idx}
            """
            params.append(limit)

            rows = await con.fetch(query, *params)

            if not rows:
                raise HTTPException(404, detail={
                    "error": "no_results",
                    "message": f'No employers found matching "{name}".',
                })

            await record_usage(key_row, "/v1/employers")
            headers = await get_quota_headers(key_row)
            return JSONResponse(
                content=_format_search_response(rows),
                headers=headers,
            )

        # Address-only search (fallback to name-based if no name given)
        raise HTTPException(400, detail={
            "error": "not_implemented",
            "message": "Address-only search requires name parameter as well.",
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
            "data": [dict(r) for r in rows],
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


def _format_search_response(rows) -> dict:
    """Format search results with match + possible_matches."""
    if not rows:
        return {"match": None, "possible_matches": []}

    employers = [_format_employer(r) for r in rows]
    return {
        "match": employers[0],
        "possible_matches": employers[1:10],  # capped at 10
    }
