"""
Quick test script to explore SAM.gov Entity API response fields.
Run on pipeline server:
    python pipeline/test_sam_api.py
"""

import json
import os
import sys
import requests

SAM_API_KEY = os.environ.get("SAM_API_KEY", "SAM-f91e8399-08d1-4357-adc0-b72cea3ad563")

BASE_URL = "https://api.sam.gov/entity-information/v3/entities"

def test_entity_search(name="AMAZON"):
    """Search by business name and dump full response structure."""
    params = {
        "api_key": SAM_API_KEY,
        "legalBusinessName": name,
        "registrationStatus": "A",  # Active only
        "includeSections": "entityRegistration,coreData",
        "page": 0,
        "size": 5,
    }
    print(f"=== Searching for: {name} ===")
    print(f"URL: {BASE_URL}")
    print(f"Params: { {k:v for k,v in params.items() if k != 'api_key'} }")
    print()

    resp = requests.get(BASE_URL, params=params, timeout=30)
    print(f"Status: {resp.status_code}")

    if resp.status_code != 200:
        print(f"Error: {resp.text[:500]}")
        return

    data = resp.json()

    # Show pagination info
    print(f"Total records: {data.get('totalRecords', '?')}")
    print()

    # Show first entity in full
    entities = data.get("entityData", data.get("entities", []))
    if not entities:
        print("No entities found in response.")
        print(f"Top-level keys: {list(data.keys())}")
        print(json.dumps(data, indent=2)[:2000])
        return

    print(f"Got {len(entities)} entities. First entity:")
    print(json.dumps(entities[0], indent=2)[:3000])
    print()

    # Summarize all top-level field paths
    print("=== Field inventory (first entity) ===")
    def walk(obj, prefix=""):
        if isinstance(obj, dict):
            for k, v in obj.items():
                path = f"{prefix}.{k}" if prefix else k
                if isinstance(v, (dict, list)):
                    walk(v, path)
                else:
                    print(f"  {path}: {repr(v)[:80]}")
        elif isinstance(obj, list) and obj:
            walk(obj[0], f"{prefix}[0]")

    walk(entities[0])


def test_extract_list():
    """Check what bulk extract files are available."""
    url = "https://api.sam.gov/data-services/v1/extracts"
    params = {
        "api_key": SAM_API_KEY,
        "fileName": "SAM_PUBLIC_MONTHLY_V2",  # List available monthly files
    }
    print("\n=== Checking bulk extract availability ===")
    resp = requests.get(url, params=params, timeout=30)
    print(f"Status: {resp.status_code}")
    if resp.status_code == 200:
        # Could be JSON or a redirect to download
        content_type = resp.headers.get("content-type", "")
        print(f"Content-Type: {content_type}")
        if "json" in content_type:
            print(json.dumps(resp.json(), indent=2)[:2000])
        else:
            print(f"Response size: {len(resp.content)} bytes")
            print(f"First 500 chars: {resp.text[:500]}")
    else:
        print(f"Error: {resp.text[:500]}")


if __name__ == "__main__":
    # Test entity search with a few names
    test_entity_search("AMAZON")
    print("\n" + "="*60 + "\n")
    test_entity_search("WALMART")
    print("\n" + "="*60 + "\n")
    test_extract_list()
