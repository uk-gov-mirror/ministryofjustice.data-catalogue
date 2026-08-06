#!/usr/bin/env python3
"""Repair and verify EM data_insights dataset-to-database links in DataHub."""

import argparse
import logging
import os
import sys

import datahub.emitter.mce_builder as mce_builder
import datahub.emitter.mcp_builder as mcp_builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.metadata.schema_classes import ContainerClass

try:
    from ingestion.config import ENV, INSTANCE, PLATFORM
    from ingestion.ingestion_utils import (
        get_cadet_metadata_json,
        parse_database_and_table_names,
        validate_fqn,
    )
except ModuleNotFoundError:
    from config import ENV, INSTANCE, PLATFORM
    from ingestion_utils import (
        get_cadet_metadata_json,
        parse_database_and_table_names,
        validate_fqn,
    )

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

IS_PART_OF_RELATIONSHIP_QUERY = """
query isPartOfRelationship($urn: String!) {
  dataset(urn: $urn) {
    relationships(input: { types: [\"IsPartOf\"], direction: OUTGOING, count: 1 }) {
      total
    }
  }
}
"""


def build_graph() -> DataHubGraph:
    datahub_gms_url = os.getenv("DATAHUB_GMS_URL")
    datahub_gms_token = os.getenv("DATAHUB_GMS_TOKEN")
    if not datahub_gms_url or not datahub_gms_token:
        raise ValueError("DATAHUB_GMS_URL and DATAHUB_GMS_TOKEN must be set")

    return DataHubGraph(
        DatahubClientConfig(server=datahub_gms_url, token=datahub_gms_token)
    )


def get_target_mappings(manifest: dict, database_name: str) -> dict[str, str]:
    """Return mapping of dataset URN -> expected database container URN."""
    mappings: dict[str, str] = {}
    for node_id, node in manifest.get("nodes", {}).items():
        del node_id
        if node.get("resource_type") not in ["model", "seed"]:
            continue

        fqn = node.get("fqn", [])
        if not validate_fqn(fqn):
            continue

        database, table_name = parse_database_and_table_names(node)
        if database != database_name:
            continue

        dataset_urn = mce_builder.make_dataset_urn_with_platform_instance(
            name=f"{database}.{table_name}",
            platform=PLATFORM,
            platform_instance=INSTANCE,
            env=ENV,
        )
        database_key = mcp_builder.DatabaseKey(
            database=database,
            platform=PLATFORM,
            instance=INSTANCE,
            env=ENV,
            backcompat_env_as_instance=True,
        )
        mappings[dataset_urn] = database_key.as_urn()

    return mappings


def has_is_part_of_relationship(graph: DataHubGraph, dataset_urn: str) -> bool:
    result = graph.execute_graphql(IS_PART_OF_RELATIONSHIP_QUERY, {"urn": dataset_urn})
    dataset = result.get("dataset") if isinstance(result, dict) else None
    relationships = dataset.get("relationships") if isinstance(dataset, dict) else None
    total = relationships.get("total", 0) if isinstance(relationships, dict) else 0
    return isinstance(total, int) and total > 0


def repair_links(graph: DataHubGraph, dataset_to_container: dict[str, str]) -> int:
    repaired = 0
    for dataset_urn, container_urn in dataset_to_container.items():
        if has_is_part_of_relationship(graph, dataset_urn):
            continue

        logger.info("Repairing missing link for dataset=%s container=%s", dataset_urn, container_urn)
        graph.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=ContainerClass(container=container_urn),
            )
        )
        repaired += 1

    return repaired


def verify_links(graph: DataHubGraph, dataset_to_container: dict[str, str]) -> list[str]:
    missing = []
    for dataset_urn in dataset_to_container:
        if not has_is_part_of_relationship(graph, dataset_urn):
            missing.append(dataset_urn)
    return missing


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest-s3-uri", required=True)
    parser.add_argument("--database", default="data_insights")
    args = parser.parse_args()

    logger.info("Using CADET instance=%s platform=%s env=%s", INSTANCE, PLATFORM, ENV)

    manifest = get_cadet_metadata_json(args.manifest_s3_uri)
    dataset_to_container = get_target_mappings(manifest, args.database)

    if not dataset_to_container:
        logger.error("No datasets found for database '%s'", args.database)
        return 1

    logger.info(
        "Loaded %d datasets for database '%s'",
        len(dataset_to_container),
        args.database,
    )

    graph = build_graph()
    repaired = repair_links(graph, dataset_to_container)
    logger.info("Repair attempted for %d datasets", repaired)

    missing_after_repair = verify_links(graph, dataset_to_container)
    if missing_after_repair:
        logger.error(
            "Still missing IsPartOf relationship for %d datasets", len(missing_after_repair)
        )
        for urn in missing_after_repair[:20]:
            logger.error("Missing link: %s", urn)
        return 2

    logger.info("All data_insights dataset links are present")
    return 0


if __name__ == "__main__":
    sys.exit(main())
