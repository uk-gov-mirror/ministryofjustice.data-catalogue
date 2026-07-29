import argparse
import logging
import os
import sys
from pathlib import Path

import datahub.emitter.mce_builder as mce_builder
import datahub.emitter.mcp_builder as mcp_builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.metadata.schema_classes import ContainerClass, StatusClass

# Ensure absolute imports like `ingestion.config` work when this file is run
# as a script via `python ingestion/fix_database_is_part_of.py`.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ingestion.config import ENV, INSTANCE, PLATFORM
from ingestion.ingestion_utils import (
    get_cadet_metadata_json,
    parse_database_and_table_names,
    validate_fqn,
)

logging.basicConfig(level=logging.INFO)

DATASET_RELATIONSHIP_QUERY = """
query datasetRelationshipCheck($urn: String!) {
    dataset(urn: $urn) {
        urn
        status {
            removed
        }
        relationships(input: { types: [\"IsPartOf\"], direction: OUTGOING, count: 1 }) {
            total
        }
    }
}
"""

CONTAINER_CONTAINS_QUERY = """
query containerContainsCheck($urn: String!) {
    container(urn: $urn) {
        urn
        relationships(input: { types: [\"Contains\"], direction: OUTGOING, count: 100 }) {
            total
        }
    }
}
"""


def emit_is_part_of_for_database(manifest_s3_uri: str, database: str) -> None:
    manifest = get_cadet_metadata_json(manifest_s3_uri)

    database_key = mcp_builder.DatabaseKey(
        database=database,
        platform=PLATFORM,
        instance=INSTANCE,
        env=ENV,
        backcompat_env_as_instance=True,
    )
    container_urn = database_key.as_urn()

    dataset_urns: set[str] = set()
    for node in manifest["nodes"].values():
        if node.get("resource_type") not in ["model", "seed"]:
            continue

        fqn = node.get("fqn", [])
        if not validate_fqn(fqn):
            continue

        node_database, table_name = parse_database_and_table_names(node)
        if node_database != database:
            continue

        dataset_urn = mce_builder.make_dataset_urn_with_platform_instance(
            name=f"{node_database}.{table_name}",
            platform=PLATFORM,
            platform_instance=INSTANCE,
            env=ENV,
        )
        dataset_urns.add(dataset_urn)

    if not dataset_urns:
        print(
            "fix_database_is_part_of: no matching datasets found "
            f"for database={database} from manifest={manifest_s3_uri}"
        )
        return

    server_config = DatahubClientConfig(
        server=os.environ["DATAHUB_GMS_URL"], token=os.environ["DATAHUB_GMS_TOKEN"]
    )
    graph = DataHubGraph(server_config)

    print(
        "fix_database_is_part_of: emitting IsPartOf links "
        f"count={len(dataset_urns)} database={database} container_urn={container_urn}"
    )

    sample_urns = sorted(dataset_urns)[:5]
    for sample in sample_urns:
        print(f"fix_database_is_part_of sample dataset_urn={sample}")

    for dataset_urn in sorted(dataset_urns):
        container_mcp = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=ContainerClass(container=container_urn),
        )
        status_mcp = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=StatusClass(removed=False),
        )
        graph.emit_mcp(container_mcp)
        graph.emit_mcp(status_mcp)

    verified_is_part_of_count = 0
    missing_dataset_count = 0
    for dataset_urn in sorted(dataset_urns):
        result = graph.execute_graphql(DATASET_RELATIONSHIP_QUERY, {"urn": dataset_urn})
        dataset_node = result.get("dataset") if isinstance(result, dict) else None
        if dataset_node is None:
            missing_dataset_count += 1
            print(
                "fix_database_is_part_of verify: dataset missing "
                f"dataset_urn={dataset_urn}"
            )
            continue

        is_part_of_total = (
            dataset_node.get("relationships", {}).get("total", 0)
            if isinstance(dataset_node.get("relationships"), dict)
            else 0
        )
        removed = (
            dataset_node.get("status", {}).get("removed")
            if isinstance(dataset_node.get("status"), dict)
            else None
        )
        if is_part_of_total > 0:
            verified_is_part_of_count += 1
        print(
            "fix_database_is_part_of verify dataset: "
            f"dataset_urn={dataset_urn} is_part_of_total={is_part_of_total} removed={removed}"
        )

    container_result = graph.execute_graphql(
        CONTAINER_CONTAINS_QUERY, {"urn": container_urn}
    )
    container_node = (
        container_result.get("container") if isinstance(container_result, dict) else None
    )
    contains_total = (
        container_node.get("relationships", {}).get("total", 0)
        if isinstance(container_node, dict)
        and isinstance(container_node.get("relationships"), dict)
        else 0
    )
    print(
        "fix_database_is_part_of verify summary: "
        f"datasets_targeted={len(dataset_urns)} "
        f"datasets_with_is_part_of={verified_is_part_of_count} "
        f"missing_datasets={missing_dataset_count} "
        f"container_contains_total={contains_total}"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--manifest-s3-uri",
        required=True,
        help="S3 URI for dbt manifest.json",
    )
    parser.add_argument(
        "--database",
        required=True,
        help="Database name to link datasets to as IsPartOf",
    )
    args = parser.parse_args()

    emit_is_part_of_for_database(
        manifest_s3_uri=args.manifest_s3_uri,
        database=args.database,
    )


if __name__ == "__main__":
    main()
