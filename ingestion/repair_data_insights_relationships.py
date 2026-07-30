import argparse
import logging
import os
from typing import Iterable

import datahub.emitter.mcp_builder as mcp_builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.metadata.schema_classes import ContainerClass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_DATASET_URNS = [
    "urn:li:dataset:(urn:li:dataPlatform:dbt,cadet_electronic_monitoring.awsdatacatalog.data_insights.position,PROD)",
    "urn:li:dataset:(urn:li:dataPlatform:dbt,cadet_electronic_monitoring.awsdatacatalog.data_insights.caseload,PROD)",
    "urn:li:dataset:(urn:li:dataPlatform:dbt,cadet_electronic_monitoring.awsdatacatalog.data_insights.curfew_atv,PROD)",
    "urn:li:dataset:(urn:li:dataPlatform:dbt,cadet_electronic_monitoring.awsdatacatalog.data_insights.daily_caseload_count,PROD)",
    "urn:li:dataset:(urn:li:dataPlatform:dbt,cadet_electronic_monitoring.awsdatacatalog.data_insights.device_activations,PROD)",
    "urn:li:dataset:(urn:li:dataPlatform:dbt,cadet_electronic_monitoring.awsdatacatalog.data_insights.device_wearer_violations,PROD)",
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Repair data_insights table container relationships by assigning known tables "
            "to the data_insights database container."
        )
    )
    parser.add_argument(
        "--dataset-urn",
        dest="dataset_urns",
        action="append",
        help="Dataset URN to repair. Can be supplied multiple times.",
    )
    parser.add_argument(
        "--dataset-database",
        default="data_insights",
        help="Database name used to derive the container URN.",
    )
    parser.add_argument(
        "--platform",
        default="dbt",
        help="DataHub platform name for the database URN.",
    )
    parser.add_argument(
        "--platform-instance",
        default=None,
        help="DataHub platform instance name for the database URN. Defaults to CADET_INSTANCE env var or 'cadet_electronic_monitoring.awsdatacatalog'.",
    )
    parser.add_argument(
        "--env",
        default="PROD",
        help="DataHub environment for the database URN.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Emit MCPs instead of only logging what would be repaired.",
    )
    return parser


def _get_graph() -> DataHubGraph:
    server_config = DatahubClientConfig(
        server=os.environ["DATAHUB_GMS_URL"],
        token=os.environ["DATAHUB_GMS_TOKEN"],
        # Force the legacy Restli path (/aspects?action=ingestProposal).
        # The OpenAPI v3 path (auto-selected when the server advertises it)
        # does not derive IsPartOf from ContainerClass in DataHub 1.6.
        openapi_ingestion=False,
    )
    return DataHubGraph(server_config)


def _get_platform_instance() -> str:
    """Get platform instance from environment or use default."""
    instance = os.environ.get("CADET_INSTANCE")
    if not instance:
        logging.warning("CADET_INSTANCE not set, defaulting to 'cadet_electronic_monitoring.awsdatacatalog'")
        instance = "cadet_electronic_monitoring.awsdatacatalog"
    return instance


def _build_database_urn(database_name: str, platform: str, platform_instance: str, env: str) -> str:
    database_key = mcp_builder.DatabaseKey(
        database=database_name,
        platform=platform,
        instance=platform_instance,
        env=env,
        backcompat_env_as_instance=True,
    )
    return database_key.as_urn()


def repair_relationships(dataset_urns: Iterable[str], database_urn: str, graph: DataHubGraph, apply: bool) -> None:
    for dataset_urn in dataset_urns:
        logger.info("Repairing container relationship for dataset urn=%s -> container urn=%s", dataset_urn, database_urn)
        if not apply:
            continue

        graph.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=ContainerClass(container=database_urn),
            )
        )
        logger.info("Emitted ContainerClass via Restli for dataset urn=%s", dataset_urn)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    dataset_urns = args.dataset_urns or DEFAULT_DATASET_URNS
    graph = _get_graph()
    
    # Use environment variable if --platform-instance not provided
    platform_instance = args.platform_instance or _get_platform_instance()
    
    database_urn = _build_database_urn(
        database_name=args.dataset_database,
        platform=args.platform,
        platform_instance=platform_instance,
        env=args.env,
    )

    repair_relationships(
        dataset_urns=dataset_urns,
        database_urn=database_urn,
        graph=graph,
        apply=args.apply,
    )


if __name__ == "__main__":
    main()
