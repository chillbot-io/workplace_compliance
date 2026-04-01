"""
pipeline/parse_addresses.py — Parse raw addresses into address_key for entity resolution.
Uses usaddress library. Writes parsed results back to DuckDB.

Usage:
    python pipeline/parse_addresses.py
"""

import os
import sys

import duckdb
import pandas as pd
import usaddress

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/data/duckdb/employer_compliance.duckdb")

# Street type abbreviation expansion
STREET_TYPES = {
    "ST": "STREET", "AVE": "AVENUE", "BLVD": "BOULEVARD", "DR": "DRIVE",
    "RD": "ROAD", "LN": "LANE", "CT": "COURT", "PL": "PLACE",
    "CIR": "CIRCLE", "WAY": "WAY", "PKWY": "PARKWAY", "HWY": "HIGHWAY",
    "TRL": "TRAIL", "SQ": "SQUARE",
}


def make_address_key(raw_address: str) -> str | None:
    """Parse a raw address into STREET_NUMBER|STREET_NAME|ZIP5 format."""
    if not raw_address or not raw_address.strip():
        return None

    try:
        tagged, _ = usaddress.tag(raw_address)
    except usaddress.RepeatedLabelError:
        return None

    number = tagged.get("AddressNumber", "").strip()
    street_parts = []
    for key in ["StreetNamePreDirectional", "StreetName", "StreetNamePostType", "StreetNamePostDirectional"]:
        if key in tagged:
            street_parts.append(tagged[key].strip())
    street = " ".join(street_parts).upper()

    # Expand abbreviations
    words = street.split()
    words = [STREET_TYPES.get(w, w) for w in words]
    street = " ".join(words)

    zip_code = tagged.get("ZipCode", "").strip()[:5]

    if not number or not street or not zip_code or len(zip_code) < 5:
        return None

    return f"{number}|{street}|{zip_code}"


def main():
    con = duckdb.connect(DUCKDB_PATH)

    # Parse OSHA addresses
    osha = con.execute("""
        SELECT activity_nr, site_address, site_city, site_state, zip5
        FROM osha_inspection_norm
        WHERE site_address IS NOT NULL
    """).df()

    print(f"Parsing {len(osha)} OSHA addresses...")
    osha["full_address"] = osha.apply(
        lambda r: f"{r['site_address']}, {r['site_city']}, {r['site_state']} {r['zip5']}"
        if pd.notna(r['site_address']) else None,
        axis=1,
    )
    osha["address_key"] = osha["full_address"].apply(make_address_key)
    parsed = osha[["activity_nr", "address_key"]].copy()

    null_count = parsed["address_key"].isna().sum()
    print(f"OSHA: {len(parsed) - null_count} parsed, {null_count} NULL address_key")

    con.register("osha_address_keys", parsed)
    con.execute("DROP TABLE IF EXISTS osha_address_keys")
    con.execute("CREATE TABLE osha_address_keys AS SELECT * FROM osha_address_keys")

    # Update osha_inspection_norm with address_key
    con.execute("""
        ALTER TABLE osha_inspection_norm ADD COLUMN IF NOT EXISTS address_key TEXT
    """)
    con.execute("""
        UPDATE osha_inspection_norm
        SET address_key = k.address_key
        FROM osha_address_keys k
        WHERE osha_inspection_norm.activity_nr = k.activity_nr
    """)

    # Parse WHD addresses
    whd = con.execute("""
        SELECT case_id, address, city, state, zip5
        FROM whd_norm
        WHERE address IS NOT NULL
    """).df()

    print(f"Parsing {len(whd)} WHD addresses...")
    whd["full_address"] = whd.apply(
        lambda r: f"{r['address']}, {r['city']}, {r['state']} {r['zip5']}"
        if pd.notna(r['address']) else None,
        axis=1,
    )
    whd["address_key"] = whd["full_address"].apply(make_address_key)
    whd_parsed = whd[["case_id", "address_key"]].copy()

    null_count = whd_parsed["address_key"].isna().sum()
    print(f"WHD: {len(whd_parsed) - null_count} parsed, {null_count} NULL address_key")

    con.register("whd_address_keys", whd_parsed)
    con.execute("DROP TABLE IF EXISTS whd_address_keys")
    con.execute("CREATE TABLE whd_address_keys AS SELECT * FROM whd_address_keys")

    con.execute("""
        ALTER TABLE whd_norm ADD COLUMN IF NOT EXISTS address_key TEXT
    """)
    con.execute("""
        UPDATE whd_norm
        SET address_key = k.address_key
        FROM whd_address_keys k
        WHERE whd_norm.case_id = k.case_id
    """)

    con.close()
    print("Address parsing complete.")


if __name__ == "__main__":
    main()
