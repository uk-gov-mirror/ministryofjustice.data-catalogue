"""
Diagnostic script: reads the EM manifest from S3 and logs the structure of
data_insights entries so we can understand how to map them to containers.

Usage:
    uv run python ingestion/diagnose_em_manifest.py \
        --manifest-uri s3://emds-prod-cadt/em_data_artefacts/prod/run_artefacts/emds-deploy-docs/latest/target/manifest.json
"""
import argparse
import json
import logging
import os

import boto3
import datahub.emitter.mce_builder as mce_builder
import datahub.emitter.mcp_builder as mcp_builder

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

TARGET_DATABASE = "data_insights"


def load_manifest(s3_uri: str) -> dict:
    s3 = boto3.client("s3")
    parts = s3_uri.replace("s3://", "").split("/", 1)
    bucket, key = parts[0], parts[1]
    response = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"), strict=False)


def summarise_node(node_id: str, node: dict, section: str, instance: str) -> None:
    schema = node.get("schema", "")
    name = node.get("name", "")
    alias = node.get("alias", "")
    identifier = node.get("identifier", "")
    fqn = node.get("fqn", [])
    resource_type = node.get("resource_type", "")
    tags = node.get("tags", [])
    database_field = node.get("database", "")

    logger.info("--- %s: %s ---", section, node_id)
    logger.info("  resource_type : %s", resource_type)
    logger.info("  database      : %s", database_field)
    logger.info("  schema        : %s", schema)
    logger.info("  name          : %s", name)
    logger.info("  alias         : %s", alias)
    logger.info("  identifier    : %s", identifier)
    logger.info("  fqn           : %s", fqn)
    logger.info("  fqn[-1]       : %s", fqn[-1] if fqn else "(empty)")
    logger.info("  tags          : %s", tags)

    # What URN would the dbt DataHub source generate?
    table = identifier or name or alias
    if schema and table:
        dataset_urn = mce_builder.make_dataset_urn_with_platform_instance(
            name=f"{schema}.{table}",
            platform="dbt",
            platform_instance=instance,
            env="PROD",
        )
        logger.info("  expected_dataset_urn : %s", dataset_urn)

        db_key = mcp_builder.DatabaseKey(
            database=schema,
            platform="dbt",
            instance=instance,
            env="PROD",
            backcompat_env_as_instance=True,
        )
        logger.info("  expected_container_urn : %s", db_key.as_urn())
    else:
        logger.info("  expected_dataset_urn : (could not derive — schema or name missing)")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest-uri", required=True)
    parser.add_argument(
        "--cadet-instance",
        default=os.environ.get("CADET_INSTANCE", "cadet_electronic_monitoring.awsdatacatalog"),
    )
    parser.add_argument("--target-database", default=TARGET_DATABASE)
    args = parser.parse_args()

    logger.info("Loading manifest from %s", args.manifest_uri)
    manifest = load_manifest(args.manifest_uri)

    logger.info("\n====== NODES (resource_type: model / seed / test) ======")
    found_nodes = 0
    for node_id, node in manifest.get("nodes", {}).items():
        schema = node.get("schema", "")
        fqn = node.get("fqn", [])
        if schema == args.target_database or args.target_database in fqn:
            found_nodes += 1
            summarise_node(node_id, node, "node", args.cadet_instance)

    logger.info("Found %d node(s) related to '%s'", found_nodes, args.target_database)

    logger.info("\n====== SOURCES (resource_type: source) ======")
    found_sources = 0
    for source_id, source in manifest.get("sources", {}).items():
        schema = source.get("schema", "")
        fqn = source.get("fqn", [])
        if schema == args.target_database or args.target_database in fqn:
            found_sources += 1
            summarise_node(source_id, source, "source", args.cadet_instance)

    logger.info("Found %d source(s) related to '%s'", found_sources, args.target_database)

    logger.info("\n====== MANIFEST TOP-LEVEL KEYS ======")
    logger.info("%s", list(manifest.keys()))

    total_nodes = len(manifest.get("nodes", {}))
    total_sources = len(manifest.get("sources", {}))
    logger.info("Total nodes: %d, Total sources: %d", total_nodes, total_sources)


if __name__ == "__main__":
    main()
