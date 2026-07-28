import logging
import re
from abc import ABCMeta
from typing import Dict, List, Optional, Union, cast

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

from ingestion.config import ENV, INSTANCE, PLATFORM, get_instance_variants
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

    def __init__(self, config: AssignCadetDatabasesConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config
        self.processed_tags = {}
        manifest = get_cadet_metadata_json(self.config.manifest_s3_uri)
        self.mappings = self._get_table_database_mappings(manifest)

        sample_mappings = list(self.mappings.items())[:5]
        logging.info(
            "AssignCadetDatabases initialized with %d table->database mappings (instance=%s; variants=%s)",
            len(self.mappings),
            INSTANCE,
            sorted(get_instance_variants(INSTANCE)),
        )
        for dataset_urn, mapping in sample_mappings:
            logging.info(
                "Sample mapping: %s -> %s",
                dataset_urn,
                mapping.get("database"),
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
        assigned_count = 0
        missing_count = 0
        for dataset_urn in self.entity_map.keys():
            container_urn = self.mappings.get(dataset_urn, {}).get("database")
            if not container_urn:
                missing_count += 1
                alternate_urn = self._toggle_instance_suffix_in_dataset_urn(dataset_urn)
                alternate_match = bool(alternate_urn and alternate_urn in self.mappings)
                logging.warning(f"No container mapping for {dataset_urn=}")
                if alternate_urn:
                    logging.warning(
                        "Tried alternate instance URN %s (present_in_mapping=%s)",
                        alternate_urn,
                        alternate_match,
                    )
                continue

            print(f"Assigning {dataset_urn=} to {container_urn=}")
            assigned_count += 1
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=f"{dataset_urn}",
                    aspect=ContainerClass(container=f"{container_urn}"),
                )
            )

        logging.info(
            "AssignCadetDatabases summary: datasets_seen=%d assigned=%d missing=%d",
            len(self.entity_map),
            assigned_count,
            missing_count,
        )

        return mcps

    def _toggle_instance_suffix_in_dataset_urn(self, dataset_urn: str) -> Optional[str]:
        """Swap between instance forms with and without `.awsdatacatalog` suffix."""
        match = re.match(
            r"^(urn:li:dataset:\(urn:li:dataPlatform:dbt,)([^.]+)(\..+,PROD\))$",
            dataset_urn,
        )
        if not match:
            return None

        prefix, instance_part, suffix = match.groups()
        suffix_to_toggle = ".awsdatacatalog"
        if instance_part.endswith(suffix_to_toggle):
            alternate_instance = instance_part.removesuffix(suffix_to_toggle)
        else:
            alternate_instance = f"{instance_part}{suffix_to_toggle}"

        return f"{prefix}{alternate_instance}{suffix}"

    @report_time
    def _get_table_database_mappings(self, manifest) -> Dict[str, Dict[str, str]]:
        mappings = {}
        instance_variants = get_instance_variants(INSTANCE)
        for node in manifest["nodes"]:
            if manifest["nodes"][node]["resource_type"] in ["model", "seed"]:
                fqn = manifest["nodes"][node]["fqn"]
                if validate_fqn(fqn):
                    database, table_name = parse_database_and_table_names(
                        manifest["nodes"][node]
                    )
                    database_key = mcp_builder.DatabaseKey(
                        database=database,
                        platform=PLATFORM,
                        instance=INSTANCE,
                        env=ENV,
                        backcompat_env_as_instance=True,
                    )
                    database_urn = database_key.as_urn()

                    for instance_variant in instance_variants:
                        dataset_urn = mce_builder.make_dataset_urn_with_platform_instance(
                            name=f"{database}.{table_name}",
                            platform=PLATFORM,
                            platform_instance=instance_variant,
                            env=ENV,
                        )
                        mappings[dataset_urn] = {
                            "database": database_urn,
                            "domain": fqn[1],
                        }

        return mappings
