#!/usr/bin/env python3
"""Validate that data_insights database and its expected tables are present in the EM manifest
and print the resulting DataHub URNs for database container and each table dataset."""

import argparse
import json
import sys

import boto3
import datahub.emitter.mce_builder as mce_builder
import datahub.emitter.mcp_builder as mcp_builder

try:
    from ingestion.config import ENV, INSTANCE, PLATFORM
except ModuleNotFoundError:
    from config import ENV, INSTANCE, PLATFORM

# Table names after stripping the schema prefix (dbt uses schema__table naming)
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


def make_container_urn(database: str) -> str:
    key = mcp_builder.DatabaseKey(
        database=database,
        platform=PLATFORM,
        instance=INSTANCE,
        env=ENV,
        backcompat_env_as_instance=True,
    )
    return key.as_urn()


def make_dataset_urn(database: str, table: str) -> str:
    return mce_builder.make_dataset_urn_with_platform_instance(
        name=f"{database}.{table}",
        platform=PLATFORM,
        platform_instance=INSTANCE,
        env=ENV,
    )


def validate(manifest: dict, database: str) -> int:
    # dbt node names use schema__table convention; table name is the part after __
    found: dict[str, str] = {}  # table_bare_name -> node name
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") not in ("model", "seed"):
            continue
        if node.get("schema", "").lower() != database.lower():
            continue
        node_name = node.get("name", "").lower()
        # strip database prefix if present (e.g. data_insights__caseload -> caseload)
        bare = node_name.removeprefix(f"{database}__")
        found[bare] = node_name

    missing = EXPECTED_TABLES - found.keys()

    container_urn = make_container_urn(database)
    print(f"\nDatabase container URN:")
    print(f"  {container_urn}")

    print(f"\nTables found in manifest ({len(found)}):")
    for bare, node_name in sorted(found.items()):
        dataset_urn = make_dataset_urn(database, bare)
        status = "✓" if bare in EXPECTED_TABLES else " "
        print(f"  [{status}] {node_name}")
        print(f"       URN: {dataset_urn}")

    if missing:
        print(f"\nFAIL: Missing expected tables: {sorted(missing)}", file=sys.stderr)
        return 1

    print(f"\nOK: All {len(EXPECTED_TABLES)} expected tables present in '{database}'")
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
