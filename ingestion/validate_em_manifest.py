#!/usr/bin/env python3
"""Validate that data_insights database and its expected tables are present in the EM manifest."""

import argparse
import json
import sys

import boto3

EXPECTED_TABLES = {
    "caseload",
    "daily_caseload_count",
    "device_activations",
    "position",
    "curfew_atv",
    "device_wearer_violations",
}


def load_manifest(manifest_s3_uri: str) -> dict:
    bucket, key = manifest_s3_uri.replace("s3://", "").split("/", 1)
    s3 = boto3.client("s3", region_name="eu-west-1")
    return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())


def validate(manifest: dict, database: str) -> int:
    found_tables: set[str] = set()
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") not in ("model", "seed"):
            continue
        # schema is the dbt schema name, which maps to the database in our naming
        if node.get("schema", "").lower() != database.lower():
            continue
        found_tables.add(node.get("name", "").lower())

    missing = EXPECTED_TABLES - found_tables

    print(f"Database '{database}' tables found in manifest: {sorted(found_tables)}")

    if missing:
        print(f"FAIL: Missing expected tables: {sorted(missing)}", file=sys.stderr)
        return 1

    print(f"OK: All {len(EXPECTED_TABLES)} expected tables present in '{database}'")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest-s3-uri", required=True)
    parser.add_argument("--database", default="data_insights")
    args = parser.parse_args()

    manifest = load_manifest(args.manifest_s3_uri)
    return validate(manifest, args.database)


if __name__ == "__main__":
    sys.exit(main())
