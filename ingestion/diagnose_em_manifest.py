"""
Diagnostic script: reads the EM manifest and checks exactly what the
AssignCadetDatabases transformer would generate as mapping keys for
data_insights entries, and what entity URNs the dbt source emits.
"""
import argparse
import json
import logging
import os
import sys

import boto3
import datahub.emitter.mce_builder as mce_builder
import datahub.emitter.mcp_builder as mcp_builder

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

TARGET_DATABASE = "data_insights"

# add parent dir to path so we can import ingestion modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def load_manifest(s3_uri: str) -> dict:
    s3 = boto3.client("s3")
    parts = s3_uri.replace("s3://", "").split("/", 1)
    bucket, key = parts[0], parts[1]
    response = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"), strict=False)


def build_mapping_urn(schema: str, table_name: str, instance: str) -> str:
    """What the AssignCadetDatabases transformer puts as the mapping key."""
    return mce_builder.make_dataset_urn_with_platform_instance(
        name=f"{schema}.{table_name}",
        platform="dbt",
        platform_instance=instance,
        env="PROD",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest-uri", required=True)
    parser.add_argument(
        "--cadet-instance",
        default=os.environ.get("CADET_INSTANCE", "cadet_electronic_monitoring.awsdatacatalog"),
    )
    parser.add_argument("--target-database", default=TARGET_DATABASE)
    args = parser.parse_args()

    logger.info("CADET_INSTANCE used: %s", args.cadet_instance)
    logger.info("Loading manifest from %s\n", args.manifest_uri)
    manifest = load_manifest(args.manifest_uri)

    # Import the parse/validate helpers from the actual ingestion code
    from ingestion.ingestion_utils import validate_fqn, parse_database_and_table_names

    logger.info("====== DATA_INSIGHTS NODES — transformer mapping analysis ======")
    for node_id, node in manifest.get("nodes", {}).items():
        schema = node.get("schema", "")
        fqn = node.get("fqn", [])
        if schema != args.target_database and args.target_database not in fqn:
            continue

        name = node.get("name", "")
        alias = node.get("alias", "")
        resource_type = node.get("resource_type", "")

        logger.info("\n--- %s ---", node_id)
        logger.info("  schema/alias/name  : %s / %s / %s", schema, alias, name)
        logger.info("  resource_type      : %s", resource_type)
        logger.info("  fqn[-1]            : %s", fqn[-1] if fqn else "")
        logger.info("  validate_fqn       : %s", validate_fqn(fqn))

        # What the transformer mapping key would be
        if validate_fqn(fqn):
            db, tbl = parse_database_and_table_names(node)
        else:
            db = schema
            tbl = node.get("identifier") or name or alias

        mapping_key = build_mapping_urn(db, tbl, args.cadet_instance)
        logger.info("  transformer mapping key  : %s", mapping_key)

        # What the dbt DataHub source emits as entity URN (uses alias when set)
        dbt_source_table = alias or name
        dbt_source_urn = build_mapping_urn(schema, dbt_source_table, args.cadet_instance)
        logger.info("  dbt source entity URN    : %s", dbt_source_urn)
        logger.info("  MATCH                    : %s", mapping_key == dbt_source_urn)

        if mapping_key != dbt_source_urn:
            logger.warning("  !!! MISMATCH — no container will be assigned !!!")

    logger.info("\n====== RECIPE platform_instance check ======")
    import yaml
    recipe_path = os.path.join(os.path.dirname(__file__), "cadet_electronic_monitoring.yaml")
    with open(recipe_path) as f:
        recipe = yaml.safe_load(f)
    recipe_instance = recipe.get("source", {}).get("config", {}).get("platform_instance", "NOT SET")
    logger.info("  recipe platform_instance : %s", recipe_instance)
    logger.info("  CADET_INSTANCE env var   : %s", args.cadet_instance)
    logger.info("  recipe == CADET_INSTANCE : %s", recipe_instance == args.cadet_instance)


if __name__ == "__main__":
    main()

