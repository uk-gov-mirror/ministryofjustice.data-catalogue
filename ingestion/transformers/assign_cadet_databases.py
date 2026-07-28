import logging
import re
from abc import ABCMeta
from typing import Dict, List, Optional, Tuple, Union, cast

import datahub.emitter.mce_builder as mce_builder
import datahub.emitter.mcp_builder as mcp_builder
from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import Aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import DatasetTransformer
from datahub.metadata.schema_classes import (
    ContainerClass,
    GlobalTagsClass,
    MetadataChangeProposalClass,
    TagAssociationClass,
)
from datahub.utilities.urns.tag_urn import TagUrn

from ingestion.config import ENV, INSTANCE, PLATFORM
from ingestion.ingestion_utils import (
    domains_to_subject_areas,
    get_cadet_metadata_json,
    parse_database_and_table_names,
    validate_fqn,
)
from ingestion.utils import report_time

logging.basicConfig(level=logging.DEBUG)


class AssignCadetDatabasesConfig(ConfigModel):
    # dataset_urn -> data product urn
    manifest_s3_uri: str


class AssignCadetDatabases(DatasetTransformer, metaclass=ABCMeta):
    """Transformer that adds database container relationship
    for a provided dataset according to a manifest"""

    ctx: PipelineContext
    config: AssignCadetDatabasesConfig
    processed_tags: Dict[str, TagAssociationClass]
    em_hotfix_database_name = "data_insights"

    def __init__(self, config: AssignCadetDatabasesConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config
        self.processed_tags = {}
        manifest = get_cadet_metadata_json(self.config.manifest_s3_uri)
        self.mappings, self.db_table_mappings = self._get_table_database_mappings(
            manifest
        )
        logging.info(
            "AssignCadetDatabases prepared mappings: urn_keys=%d db_table_keys=%d instance=%s",
            len(self.mappings),
            len(self.db_table_mappings),
            INSTANCE,
        )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AssignCadetDatabases":
        config = AssignCadetDatabasesConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def aspect_name(self):
        return "globalTags"

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        if aspect is None:
            return None
        in_global_tags_aspect: GlobalTagsClass = cast(GlobalTagsClass, aspect)
        domain = self.mappings.get(entity_urn, {}).get("domain")
        if domain:
            subject_area = domains_to_subject_areas.get(domain.lower())
            subject_area_tag_urn = (
                mce_builder.make_tag_urn(tag=subject_area) if subject_area else None
            )
            existing_tags = [tag.tag for tag in in_global_tags_aspect.tags]
            # Check if the tag already exists
            if subject_area_tag_urn and subject_area_tag_urn not in existing_tags:
                tags_to_add = [
                    TagAssociationClass(tag=subject_area_tag_urn),
                ]
                in_global_tags_aspect.tags.extend(tags_to_add)
                # Keep track of tags added so that we can create them in handle_end_of_stream
                for tag in tags_to_add:
                    self.processed_tags.setdefault(tag.tag, tag)

        return cast(Aspect, in_global_tags_aspect)

    @report_time
    def handle_end_of_stream(
        self,
    ) -> List[Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]]:

        mcps: List[
            Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]
        ] = []

        print("Generating tags")

        for tag_association in self.processed_tags.values():
            tag_urn = TagUrn.from_string(tag_association.tag)
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=tag_urn.urn(),
                    aspect=tag_urn.to_key_aspect(),
                )
            )

        print("Assigning datasets to databases")
        for dataset_urn in self.entity_map.keys():
            mapping = self.mappings.get(dataset_urn)
            parsed = self._parse_dataset_urn_for_database_table(dataset_urn)
            if not mapping:
                if parsed:
                    mapping = self.db_table_mappings.get(parsed)
                    if mapping:
                        logging.info(
                            "Recovered container mapping by database/table fallback for dataset_urn=%s database=%s table=%s",
                            dataset_urn,
                            parsed[0],
                            parsed[1],
                        )

            container_urn = mapping.get("database") if mapping else None
            if (
                not container_urn
                and parsed
                and parsed[0] == self.em_hotfix_database_name
                and "cadet_electronic_monitoring.awsdatacatalog." in dataset_urn
            ):
                container_urn = self._make_database_container_urn(parsed[0])
                logging.info(
                    "Applied EM data_insights hotfix mapping for dataset_urn=%s -> container_urn=%s",
                    dataset_urn,
                    container_urn,
                )

            if not container_urn:
                logging.warning(
                    "No container mapping for dataset_urn=%s parsed_database_table=%s",
                    dataset_urn,
                    parsed,
                )
                continue

            print(f"Assigning {dataset_urn=} to {container_urn=}")
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=f"{dataset_urn}",
                    aspect=ContainerClass(container=f"{container_urn}"),
                )
            )

        return mcps

    @report_time
    def _get_table_database_mappings(
        self, manifest
    ) -> Tuple[Dict[str, Dict[str, str]], Dict[Tuple[str, str], Dict[str, str]]]:
        mappings = {}
        db_table_mappings: Dict[Tuple[str, str], Dict[str, str]] = {}
        for node in manifest["nodes"]:
            if manifest["nodes"][node]["resource_type"] in ["model", "seed"]:
                fqn = manifest["nodes"][node]["fqn"]
                if validate_fqn(fqn):
                    database, table_name = parse_database_and_table_names(
                        manifest["nodes"][node]
                    )

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
                    database_urn = database_key.as_urn()

                    mapping = {"database": database_urn, "domain": fqn[1]}
                    mappings[dataset_urn] = mapping
                    db_table_mappings[(database, table_name)] = mapping

        return mappings, db_table_mappings

    def _parse_dataset_urn_for_database_table(
        self, dataset_urn: str
    ) -> Optional[Tuple[str, str]]:
        """Extract database and table from DataHub dataset URN name segment."""
        match = re.match(r"^urn:li:dataset:\(urn:li:dataPlatform:[^,]+,([^,]+),[^\)]+\)$", dataset_urn)
        if not match:
            return None

        dataset_name = match.group(1)
        parts = dataset_name.split(".")
        if len(parts) < 3:
            return None

        return parts[-2], parts[-1]

    def _make_database_container_urn(self, database: str) -> str:
        database_key = mcp_builder.DatabaseKey(
            database=database,
            platform=PLATFORM,
            instance=INSTANCE,
            env=ENV,
            backcompat_env_as_instance=True,
        )
        return database_key.as_urn()
