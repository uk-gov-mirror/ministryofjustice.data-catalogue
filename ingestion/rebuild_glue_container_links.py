import argparse
import os

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import DatabaseKey
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.metadata.schema_classes import ChangeTypeClass, ContainerClass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay dataset->container links for a Glue database."
    )
    parser.add_argument(
        "--database",
        required=True,
        help="Glue database name to scope the replay, e.g. dlpes_dwp_mini_datashare",
    )
    parser.add_argument(
        "--platform",
        default="glue",
        help="Data platform for dataset filtering (default: glue)",
    )
    parser.add_argument(
        "--env",
        default="PROD",
        help="DataHub environment for dataset filtering (default: PROD)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Batch size for DataHub dataset search (default: 500)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview matching dataset URNs without writing MCPs",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    gms_url = os.getenv("DATAHUB_GMS_URL")
    gms_token = os.getenv("DATAHUB_GMS_TOKEN")
    if not gms_url or not gms_token:
        raise ValueError(
            "Both DATAHUB_GMS_URL and DATAHUB_GMS_TOKEN must be set in the environment."
        )

    graph = DataHubGraph(
        DatahubClientConfig(server=gms_url, token=gms_token)
    )
    emitter = DatahubRestEmitter(gms_server=graph.config.server, token=graph.config.token)

    container_urn = DatabaseKey(
        database=args.database,
        platform=args.platform,
        instance=None,
        env=args.env,
        backcompat_env_as_instance=True,
    ).as_urn()

    matched = 0
    emitted = 0

    for urn in graph.get_urns_by_filter(
        entity_types=["dataset"],
        platform=args.platform,
        env=args.env,
        query=args.database,
        batch_size=args.batch_size,
    ):
        if f",{args.database}." not in urn:
            continue

        matched += 1
        if args.dry_run:
            print(f"[DRY_RUN] {urn}")
            continue

        mcp = MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=ContainerClass(container=container_urn),
            changeType=ChangeTypeClass.UPSERT,
        )
        emitter.emit_mcp(mcp)
        emitted += 1

    print(
        "Summary: "
        f"matched={matched} emitted={emitted} dry_run={args.dry_run} "
        f"container_urn={container_urn}"
    )


if __name__ == "__main__":
    main()