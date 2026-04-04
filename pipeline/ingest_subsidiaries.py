"""
pipeline/ingest_subsidiaries.py — Download SEC Exhibit 21 subsidiary data
from OpenSanctions CorpWatch dataset and generate a parent company seed.

This is a structured, machine-readable dataset of parent→subsidiary
relationships parsed from SEC 10-K Exhibit 21 filings. Covers all
publicly-listed US companies and their subsidiaries.

Generates: dbt/seeds/parent_companies.csv
    columns: name_pattern, parent_name

Usage:
    python pipeline/ingest_subsidiaries.py

No API key needed — OpenSanctions data is free for non-commercial use.
"""

import csv
import io
import json
import os
import re
import sys
import zipfile
from pathlib import Path

import requests as req
import pandas as pd

# OpenSanctions CorpWatch dataset — Exhibit 21 subsidiary relationships
DATASET_URL = "https://data.opensanctions.org/datasets/latest/us_corpwatch/entities.ftm.json"

OUTPUT_PATH = Path("dbt/seeds/parent_companies.csv")


def normalize_name(name: str) -> str:
    """Normalize company name for matching against OSHA records."""
    if not name:
        return ""
    name = name.upper().strip()
    # Remove punctuation
    name = re.sub(r'[^A-Z0-9 ]', '', name)
    # Remove common suffixes
    for suffix in ['INC', 'INCORPORATED', 'LLC', 'LC', 'LLP', 'LP', 'LTD',
                   'LIMITED', 'CORP', 'CORPORATION', 'CO', 'COMPANY',
                   'COMPANIES', 'DBA', 'DOING BUSINESS AS', 'GROUP',
                   'HOLDINGS', 'HOLDING']:
        name = re.sub(rf'( |^){suffix}( |$)', ' ', name)
    # Remove trailing numbers
    name = re.sub(r'[0-9]+$', '', name)
    # Collapse whitespace
    name = re.sub(r' +', ' ', name).strip()
    return name


def download_corpwatch() -> list[dict]:
    """Download the CorpWatch FtM entities and extract parent-subsidiary pairs."""
    print("Downloading OpenSanctions CorpWatch dataset...")
    print(f"  URL: {DATASET_URL}")

    resp = req.get(DATASET_URL, timeout=120, stream=True)
    resp.raise_for_status()

    # FtM format: one JSON object per line
    entities = {}
    ownership_links = []

    print("  Parsing entities...")
    for line in resp.iter_lines():
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue

        schema = obj.get("schema")
        entity_id = obj.get("id")
        props = obj.get("properties", {})

        if schema == "Company":
            name = (props.get("name") or [""])[0]
            if name:
                entities[entity_id] = {
                    "name": name,
                    "country": (props.get("country") or [""])[0],
                    "jurisdiction": (props.get("jurisdiction") or [""])[0],
                }

        elif schema == "Ownership":
            owner_ids = props.get("owner", [])
            asset_ids = props.get("asset", [])
            for owner_id in owner_ids:
                for asset_id in asset_ids:
                    ownership_links.append((owner_id, asset_id))

    print(f"  {len(entities)} companies, {len(ownership_links)} ownership links")
    return entities, ownership_links


def build_parent_map(entities: dict, links: list) -> pd.DataFrame:
    """Build parent→subsidiary name mapping from ownership links."""
    rows = []

    for owner_id, asset_id in links:
        owner = entities.get(owner_id)
        subsidiary = entities.get(asset_id)
        if not owner or not subsidiary:
            continue

        parent_name = owner["name"]
        sub_name = subsidiary["name"]

        # Normalize the subsidiary name for matching
        name_pattern = normalize_name(sub_name)
        parent_normalized = normalize_name(parent_name)

        if not name_pattern or not parent_normalized:
            continue

        # Skip if subsidiary name is basically the same as parent
        if name_pattern == parent_normalized:
            continue

        rows.append({
            "name_pattern": name_pattern,
            "parent_name": parent_name,
        })

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    # Deduplicate — keep one parent per subsidiary name pattern
    df = df.drop_duplicates(subset=["name_pattern"])
    df["match_type"] = "exact"

    # Also add the parent company itself as a pattern
    parent_self_rows = []
    for owner_id, _ in links:
        owner = entities.get(owner_id)
        if owner:
            parent_normalized = normalize_name(owner["name"])
            if parent_normalized:
                parent_self_rows.append({
                    "name_pattern": parent_normalized,
                    "parent_name": owner["name"],
                })

    if parent_self_rows:
        parent_df = pd.DataFrame(parent_self_rows).drop_duplicates(subset=["name_pattern"])
        df = pd.concat([df, parent_df]).drop_duplicates(subset=["name_pattern"], keep="first")

    print(f"  {len(df)} unique name patterns mapped to parents")
    return df


def main():
    print("=== SEC Exhibit 21 Subsidiary Ingestion ===\n")

    entities, links = download_corpwatch()

    if not links:
        print("ERROR: No ownership links found", file=sys.stderr)
        sys.exit(1)

    df = build_parent_map(entities, links)

    if df.empty:
        print("ERROR: No parent-subsidiary mappings generated", file=sys.stderr)
        sys.exit(1)

    # Append manual overrides for national chains that SEC data misses
    # (OSHA establishment names don't match SEC subsidiary names exactly)
    overrides = [
        # Amazon variants in OSHA data
        ("AMAZON COM SERVICES", "Amazon.com, Inc."),
        ("AMAZONCOM SERVICES", "Amazon.com, Inc."),
        ("AMAZONCOM DELIVERY SERVICES", "Amazon.com, Inc."),
        ("AMAZON FULFILLMENT", "Amazon.com, Inc."),
        ("AMAZON LOGISTICS", "Amazon.com, Inc."),
        ("AMAZON WAREHOUSE", "Amazon.com, Inc."),
        ("AMAZON DMS", "Amazon.com, Inc."),
        ("AMAZON DELIVERY", "Amazon.com, Inc."),
        ("WHOLE FOODS MARKET", "Amazon.com, Inc."),
        # Walmart
        ("WALMART", "Walmart Inc."),
        ("WAL MART", "Walmart Inc."),
        ("SAMS CLUB", "Walmart Inc."),
        # Target
        ("TARGET", "Target Corporation"),
        # Kroger
        ("KROGER", "The Kroger Co."),
        ("FRED MEYER", "The Kroger Co."),
        ("RALPHS GROCERY", "The Kroger Co."),
        ("HARRIS TEETER", "The Kroger Co."),
        # Costco
        ("COSTCO", "Costco Wholesale Corporation"),
        # Home improvement
        ("HOME DEPOT", "The Home Depot, Inc."),
        ("LOWES", "Lowe's Companies, Inc."),
        # Fast food / restaurants
        ("MCDONALDS", "McDonald's Corporation"),
        ("STARBUCKS", "Starbucks Corporation"),
        ("CHIPOTLE", "Chipotle Mexican Grill, Inc."),
        ("SUBWAY", "Subway IP LLC"),
        # Dollar stores
        ("DOLLAR GENERAL", "Dollar General Corporation"),
        ("DOLLAR TREE", "Dollar Tree, Inc."),
        ("FAMILY DOLLAR", "Dollar Tree, Inc."),
        # Shipping
        ("FEDEX", "FedEx Corporation"),
        ("FEDERAL EXPRESS", "FedEx Corporation"),
        ("FEDEX GROUND", "FedEx Corporation"),
        ("FEDEX FREIGHT", "FedEx Corporation"),
        ("UNITED PARCEL", "United Parcel Service, Inc."),
        # Meat processing
        ("TYSON FOODS", "Tyson Foods, Inc."),
        ("TYSON FRESH MEATS", "Tyson Foods, Inc."),
        ("PILGRIMS PRIDE", "Pilgrim's Pride Corporation"),
        ("JBS USA", "JBS USA Holdings, Inc."),
        ("SMITHFIELD FOODS", "Smithfield Foods, Inc."),
        ("SMITHFIELD FRESH", "Smithfield Foods, Inc."),
        ("CARGILL MEAT", "Cargill, Incorporated"),
        ("CARGILL", "Cargill, Incorporated"),
        # Pharmacy / health
        ("CVS PHARMACY", "CVS Health Corporation"),
        ("CVS HEALTH", "CVS Health Corporation"),
        ("WALGREENS", "Walgreens Boots Alliance, Inc."),
        # Auto
        ("TESLA", "Tesla, Inc."),
        ("GENERAL MOTORS", "General Motors Company"),
        ("FORD MOTOR", "Ford Motor Company"),
        # Waste
        ("WASTE MANAGEMENT", "Waste Management, Inc."),
        ("REPUBLIC SERVICES", "Republic Services, Inc."),
    ]

    override_df = pd.DataFrame(overrides, columns=["name_pattern", "parent_name"])
    override_df["match_type"] = "prefix"
    before = len(df)
    df = pd.concat([override_df, df]).drop_duplicates(subset=["name_pattern"], keep="first")
    print(f"  Added {len(df) - before + len(override_df)} manual overrides (national chains)")

    # Clean ALL string columns — remove characters that break dbt/DuckDB CSV sniffer
    for col in ["name_pattern", "parent_name"]:
        df[col] = (df[col]
            .str.replace(",", "", regex=False)
            .str.replace("'", "", regex=False)
            .str.replace('"', "", regex=False)
            .str.replace("\\", "", regex=False)
            .str.replace("#", "", regex=False)
        )

    # Verify no problematic characters remain
    for col in ["name_pattern", "parent_name"]:
        bad = df[col].str.contains(r"[,'\"\\\#]", regex=True, na=False)
        if bad.any():
            print(f"  WARNING: {bad.sum()} rows in {col} still have special chars, dropping them")
            df = df[~bad]

    # Save as dbt seed
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"\nSaved {len(df)} mappings to {OUTPUT_PATH}")

    # Show some examples
    print("\nSample mappings:")
    for _, row in df.head(20).iterrows():
        print(f"  {row['name_pattern']:40s} → {row['parent_name']}")


if __name__ == "__main__":
    main()
