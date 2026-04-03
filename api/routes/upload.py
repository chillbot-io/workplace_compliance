"""
api/routes/upload.py — CSV bulk upload endpoint.
Accepts a CSV file with employer names + optional location fields,
runs batch matching, returns a CSV with risk profiles appended.
"""

import csv
import io
import json
from decimal import Decimal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse

from api.auth import check_scope, check_monthly_quota, record_usage, get_quota_headers, get_pool
from api.routes.employers import _resolve_batch_item, _match_confidence, _format_employer, BatchLookupItem


router = APIRouter(prefix="/v1")

# Max rows in a single CSV upload
CSV_MAX_ROWS = 500
CSV_MAX_SIZE_MB = 5


@router.post("/employers/upload-csv")
async def upload_csv(
    file: UploadFile = File(..., description="CSV file with employer names and optional location fields"),
    key_row=Depends(check_scope("batch:write")),
):
    """Upload a CSV of employers, get back a CSV with risk profiles.

    Expected CSV columns (flexible — matches by header name):
        Required: name OR company_name OR employer_name
        Optional: state, zip, city, ein

    Extra columns in the input are preserved in the output.
    Output adds: employer_id, match_confidence, risk_tier, risk_score,
    osha_inspections_5yr, osha_violations_5yr, osha_total_penalties,
    location_count, trend_signal
    """
    # Validate file type
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(400, detail={
            "error": "invalid_file_type",
            "message": "Upload a .csv file.",
        })

    # Read file content with size limit
    contents = await file.read()
    if len(contents) > CSV_MAX_SIZE_MB * 1024 * 1024:
        raise HTTPException(413, detail={
            "error": "file_too_large",
            "message": f"CSV must be under {CSV_MAX_SIZE_MB}MB.",
        })

    # Parse CSV
    try:
        text = contents.decode("utf-8-sig")  # handles BOM from Excel
    except UnicodeDecodeError:
        try:
            text = contents.decode("latin-1")
        except UnicodeDecodeError:
            raise HTTPException(400, detail={
                "error": "invalid_encoding",
                "message": "CSV must be UTF-8 or Latin-1 encoded.",
            })

    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise HTTPException(400, detail={
            "error": "empty_csv",
            "message": "CSV has no headers.",
        })

    # Normalize headers (lowercase, strip whitespace)
    header_map = {h.strip().lower().replace(" ", "_"): h for h in reader.fieldnames}

    # Find the name column
    name_col = None
    for candidate in ["name", "company_name", "employer_name", "company", "employer", "business_name", "trade_name"]:
        if candidate in header_map:
            name_col = header_map[candidate]
            break

    ein_col = header_map.get("ein", header_map.get("fein", header_map.get("tax_id")))
    state_col = header_map.get("state", header_map.get("st", header_map.get("state_code")))
    zip_col = header_map.get("zip", header_map.get("zip_code", header_map.get("zipcode", header_map.get("postal_code"))))
    city_col = header_map.get("city", header_map.get("city_name"))

    if not name_col and not ein_col:
        raise HTTPException(400, detail={
            "error": "missing_columns",
            "message": f"CSV must have a name column (name, company_name, employer_name) or ein column. Found: {', '.join(reader.fieldnames)}",
        })

    # Read all rows
    rows = list(reader)
    if len(rows) > CSV_MAX_ROWS:
        raise HTTPException(422, detail={
            "error": "too_many_rows",
            "message": f"Maximum {CSV_MAX_ROWS} rows per upload. Got {len(rows)}.",
        })

    if not rows:
        raise HTTPException(400, detail={
            "error": "empty_csv",
            "message": "CSV has no data rows.",
        })

    await check_monthly_quota(key_row)

    # Process each row
    output_rows = []
    matched = 0

    async with get_pool().acquire() as con:
        for row in rows:
            item = BatchLookupItem(
                name=row.get(name_col, "").strip() if name_col else None,
                ein=row.get(ein_col, "").strip() if ein_col else None,
                state=row.get(state_col, "").strip() if state_col else None,
                zip=row.get(zip_col, "").strip() if zip_col else None,
                city=row.get(city_col, "").strip() if city_col else None,
            )

            # Skip empty rows
            if not item.name and not item.ein:
                out = dict(row)
                out.update(_empty_result_columns())
                output_rows.append(out)
                continue

            match = await _resolve_batch_item(item, con)

            out = dict(row)  # preserve original columns
            if match:
                matched += 1
                emp = _format_employer(match)
                out["fastdol_employer_id"] = emp.get("employer_id", "")
                out["fastdol_match_confidence"] = _match_confidence(match)
                out["fastdol_risk_tier"] = emp.get("risk_tier", "")
                out["fastdol_risk_score"] = emp.get("risk_score", "")
                out["fastdol_osha_inspections_5yr"] = emp.get("osha_inspections_5yr", "")
                out["fastdol_osha_violations_5yr"] = emp.get("osha_violations_5yr", "")
                out["fastdol_osha_total_penalties"] = emp.get("osha_total_penalties", "")
                out["fastdol_location_count"] = emp.get("location_count", "")
                out["fastdol_trend_signal"] = emp.get("trend_signal", "")
                out["fastdol_matched_name"] = emp.get("employer_name", "")
                out["fastdol_matched_address"] = emp.get("address", "")
                out["fastdol_matched_city"] = emp.get("city", "")
                out["fastdol_matched_state"] = emp.get("state", "")
                out["fastdol_matched_zip"] = emp.get("zip", "")
            else:
                out.update(_empty_result_columns())

            output_rows.append(out)

    await record_usage(key_row, "/v1/employers/upload-csv", count=len(rows))

    # Build output CSV
    if not output_rows:
        raise HTTPException(500, detail={"error": "internal", "message": "No output generated."})

    output_buffer = io.StringIO()
    fieldnames = list(output_rows[0].keys())
    writer = csv.DictWriter(output_buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(output_rows)

    output_buffer.seek(0)

    headers = await get_quota_headers(key_row)
    headers["X-Matched"] = str(matched)
    headers["X-Total"] = str(len(rows))

    return StreamingResponse(
        io.BytesIO(output_buffer.getvalue().encode("utf-8")),
        media_type="text/csv",
        headers={
            **headers,
            "Content-Disposition": "attachment; filename=fastdol_results.csv",
        },
    )


def _empty_result_columns() -> dict:
    """Empty FastDOL columns for unmatched rows."""
    return {
        "fastdol_employer_id": "",
        "fastdol_match_confidence": "",
        "fastdol_risk_tier": "",
        "fastdol_risk_score": "",
        "fastdol_osha_inspections_5yr": "",
        "fastdol_osha_violations_5yr": "",
        "fastdol_osha_total_penalties": "",
        "fastdol_location_count": "",
        "fastdol_trend_signal": "",
        "fastdol_matched_name": "",
        "fastdol_matched_address": "",
        "fastdol_matched_city": "",
        "fastdol_matched_state": "",
        "fastdol_matched_zip": "",
    }
