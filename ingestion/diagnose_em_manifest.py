"""
Diagnostic script for EM data_insights container relationships.
Run with --verify-relationships after ingest to check DataHub state.
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

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def load_manifest(s3_uri: str) -> dict:
    s3 = boto3.client("s3")
    parts = s3_uri.replace("s3://", "").split("/", 1)
    bucket, key = parts[0], parts[1]
    response = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"), strict=False)


def build_mapping_urn(schema: str, table_name: str, instance: str) -> str:
    return mce_builder.make_dataset_urn_with_platform_instance(
        name=f"{schema}.{table_name}",
        platform="dbt",
        platform_instance=instance,
        env="PROD",
    )


def check_relationships(dataset_urns: list[str], expected_container_urn: str) -> None:
    from datahub.ingestion.graph.client import DataHubGraph
    from datahub.ingestion.graph.config import DatahubClientConfig
    from datahub.ingestion.graph.openapi import RelationshipDirection

    graph = DataHubGraph(DatahubClientConfig(
        server=os.environ["DATAHUB_GMS_URL"],
        token=os.environ["DATAHUB_GMS_TOKEN"],
    ))

    is_part_of_query = """
        query($urn: String!) {
            dataset(urn: $urn) {
                relationships(input: {types: ["IsPartOf"], direction: OUTGOING, count: 5}) {
                    total
                    relationships {
                        entity { urn }
                    }
                }
                container { urn }
            }
        }
    """

    # Query the container entity itself to check existence and INCOMING children
    container_query = """
        query($urn: String!) {
            container(urn: $urn) {
                urn
                exists
                status { removed }
                relationships(input: {types: ["IsPartOf"], direction: INCOMING, start: 0, count: 10}) {
                    total
                }
            }
        }
    """

    try:
        container_result = graph.execute_graphql(container_query, {"urn": expected_container_urn})
        c = container_result.get("container") or {}
        c_exists = c.get("exists", False)
        c_removed = (c.get("status") or {}).get("removed", False)
        c_incoming = (c.get("relationships") or {}).get("total", 0)
        logger.info("  container exists        : %s", c_exists)
        logger.info("  container soft-deleted  : %s", c_removed)
        logger.info("  container INCOMING total: %d", c_incoming)
    except Exception as exc:
        logger.error("  ERROR querying container entity: %s", exc)

    for dataset_urn in dataset_urns:
        try:
            result = graph.execute_graphql(is_part_of_query, {"urn": dataset_urn})
            dataset_result = result.get("dataset") or {}
            relationships = dataset_result.get("relationships") or {}
            container = dataset_result.get("container") or {}

            total = relationships.get("total", 0)
            linked_urns = [r["entity"]["urn"] for r in relationships.get("relationships", [])]
            direct_container = container.get("urn", "none")
            linked_to_expected = expected_container_urn in linked_urns

            logger.info("  %s", dataset_urn.split("data_insights.")[-1])
            logger.info("    IsPartOf total      : %d", total)
            logger.info("    linked containers   : %s", linked_urns or "[]")
            logger.info("    direct container    : %s", direct_container)
            logger.info("    linked to expected  : %s  (expected: %s)", linked_to_expected, expected_container_urn)

            # Scroll ALL relationship types to detect if edges exist under a different type name
            all_rels = list(graph.scroll_relationships(
                source_urns=[dataset_urn],
                direction=RelationshipDirection.OUTGOING,
                count=20,
            ).relationships)
            rel_types = [(r.relationship_type, r.destination_urn) for r in all_rels]
            logger.info("    ALL outgoing rels   : %s", rel_types or "[]")
        except Exception as exc:
            logger.error("    ERROR querying %s: %s", dataset_urn, exc)

            logger.info("  %s", dataset_urn.split("data_insights.")[-1])
            logger.info("    IsPartOf total      : %d", total)
            logger.info("    linked containers   : %s", linked_urns or "[]")
            logger.info("    direct container    : %s", direct_container)
            logger.info("    linked to expected  : %s  (expected: %s)", linked_to_expected, expected_container_urn)
        except Exception as exc:
            logger.error("    ERROR querying %s: %s", dataset_urn, exc)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest-uri", required=True)
    parser.add_argument(
        "--cadet-instance",
        default=os.environ.get("CADET_INSTANCE", "cadet_electronic_monitoring.awsdatacatalog"),
    )
    parser.add_argument("--target-database", default=TARGET_DATABASE)
    parser.add_argument("--verify-relationships", action="store_true")
    args = parser.parse_args()

    manifest = load_manifest(args.manifest_uri)

    from ingestion.ingestion_utils import validate_fqn, parse_database_and_table_names

    dataset_urns = []
    container_urn = None

    logger.info("====== DATA_INSIGHTS NODES — transformer mapping analysis ======")
    for node_id, node in manifest.get("nodes", {}).items():
        schema = node.get("schema", "")
        fqn = node.get("fqn", [])
        if schema != args.target_database and args.target_database not in fqn:
            continue

        alias = node.get("alias", "")
        name = node.get("name", "")

        if validate_fqn(fqn):
            db, tbl = parse_database_and_table_names(node)
        else:
            db = schema
            tbl = node.get("identifier") or name or alias

        mapping_key = build_mapping_urn(db, tbl, args.cadet_instance)
        dbt_source_urn = build_mapping_urn(schema, alias or name, args.cadet_instance)
        match = mapping_key == dbt_source_urn

        logger.info("\n--- %s ---", node_id)
        logger.info("  schema/alias  : %s / %s", schema, alias)
        logger.info("  transformer mapping key : %s", mapping_key)
        logger.info("  dbt source entity URN   : %s", dbt_source_urn)
        logger.info("  MATCH                   : %s", match)

        dataset_urns.append(dbt_source_urn)

        db_key = mcp_builder.DatabaseKey(
            database=db, platform="dbt", instance=args.cadet_instance,
            env="PROD", backcompat_env_as_instance=True,
        )
        container_urn = db_key.as_urn()

    import yaml
    recipe_path = os.path.join(os.path.dirname(__file__), "cadet_electronic_monitoring.yaml")
    with open(recipe_path) as f:
        recipe = yaml.safe_load(f)
    recipe_instance = recipe.get("source", {}).get("config", {}).get("platform_instance", "NOT SET")
    logger.info("\n====== RECIPE CHECK ======")
    logger.info("  recipe platform_instance : %s", recipe_instance)
    logger.info("  CADET_INSTANCE env var   : %s", args.cadet_instance)
    logger.info("  recipe == CADET_INSTANCE : %s", recipe_instance == args.cadet_instance)
    logger.info("  container URN            : %s", container_urn)

    if args.verify_relationships:
        logger.info("\n====== POST-INGEST RELATIONSHIP VERIFICATION ======")
        if not os.environ.get("DATAHUB_GMS_URL") or not os.environ.get("DATAHUB_GMS_TOKEN"):
            logger.error("DATAHUB_GMS_URL / DATAHUB_GMS_TOKEN not set — skipping")
        else:
            check_relationships(dataset_urns, container_urn)


if __name__ == "__main__":
    main()

