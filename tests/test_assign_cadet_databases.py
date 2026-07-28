from typing import cast

import datahub.emitter.mce_builder as builder
import datahub.emitter.mcp_builder as mcp_builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import EndOfStream, PipelineContext, RecordEnvelope
from datahub.ingestion.graph.client import DatahubClientConfig
from datahub.metadata.schema_classes import TagAssociationClass
from utils import make_generic_dataset_mcp, run_dataset_transformer_pipeline

from ingestion.config import ENV, INSTANCE, PLATFORM
from ingestion.transformers.assign_cadet_databases import AssignCadetDatabases


class TestAssignCadetDatabasesTransformer:
    def test_em_data_insights_hotfix_assigns_container(self, mock_datahub_graph):
        pipeline_context: PipelineContext = PipelineContext(run_id="abc")
        pipeline_context.graph = mock_datahub_graph(DatahubClientConfig)
        transformer = AssignCadetDatabases.create(
            {
                "manifest_s3_uri": "s3://test_bucket/prod/run_artefacts/latest/target/manifest.json",
            },
            pipeline_context,
        )

        em_dataset_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:dbt,"
            "cadet_electronic_monitoring.awsdatacatalog.data_insights.caseload,PROD)"
        )
        dataset_mcp = make_generic_dataset_mcp(
            entity_urn=em_dataset_urn,
            aspect_name=transformer.aspect_name(),
            aspect=models.GlobalTagsClass(tags=[]),
        )

        outputs = list(
            transformer.transform(
                [
                    RecordEnvelope(dataset_mcp, metadata={}),
                    RecordEnvelope(EndOfStream(), metadata={}),
                ]
            )
        )

        expected_container = mcp_builder.DatabaseKey(
            database="data_insights",
            platform=PLATFORM,
            instance=INSTANCE,
            env=ENV,
            backcompat_env_as_instance=True,
        ).as_urn()

        container_mcps = [
            record.record
            for record in outputs
            if record.record
            and isinstance(record.record, MetadataChangeProposalWrapper)
            and isinstance(record.record.aspect, models.ContainerClass)
            and record.record.entityUrn == em_dataset_urn
        ]

        assert len(container_mcps) == 1
        assert container_mcps[0].aspect.container == expected_container

    def test_em_urn_parses_to_database_table_fallback_key(self, mock_datahub_graph):
        pipeline_context: PipelineContext = PipelineContext(run_id="abc")
        pipeline_context.graph = mock_datahub_graph(DatahubClientConfig)
        transformer = AssignCadetDatabases.create(
            {
                "manifest_s3_uri": "s3://test_bucket/prod/run_artefacts/latest/target/manifest.json",
            },
            pipeline_context,
        )

        parsed = transformer._parse_dataset_urn_for_database_table(
            "urn:li:dataset:(urn:li:dataPlatform:dbt,cadet_electronic_monitoring.awsdatacatalog.prison_database.table2,PROD)"
        )

        assert parsed == ("prison_database", "table2")
        assert transformer.db_table_mappings.get(parsed) is not None

    def test_pattern_add_dataset_domain_match(self, mock_datahub_graph):
        pipeline_context: PipelineContext = PipelineContext(run_id="abc")
        pipeline_context.graph = mock_datahub_graph(DatahubClientConfig)
        expected_key = mcp_builder.DatabaseKey(
            database="prison_database",
            platform=PLATFORM,
            instance=INSTANCE,
            env=ENV,
            backcompat_env_as_instance=True,
        )

        output = run_dataset_transformer_pipeline(
            transformer_type=AssignCadetDatabases,
            aspect=models.GlobalTagsClass(tags=[]),
            config={
                "manifest_s3_uri": "s3://test_bucket/prod/run_artefacts/latest/target/manifest.json",
            },
            pipeline_context=pipeline_context,
        )

        assert len(output) == 4
        assert output[0] is not None
        assert output[0].record is not None
        assert isinstance(output[0].record, MetadataChangeProposalWrapper)
        assert output[0].record.aspect is not None
        assert isinstance(output[0].record.aspect, models.GlobalTagsClass)
        assert output[0].record.aspect.tags == [
            TagAssociationClass(tag=builder.make_tag_urn("Prisons and probation")),
        ]
        assert isinstance(output[2].record.aspect, models.ContainerClass)
        assert output[2].record.aspect.container == expected_key.as_urn()

    def test_pattern_add_dataset_domain_match_aspect_none(self, mock_datahub_graph):
        pipeline_context: PipelineContext = PipelineContext(run_id="abc")
        pipeline_context.graph = mock_datahub_graph(DatahubClientConfig)

        output = run_dataset_transformer_pipeline(
            transformer_type=AssignCadetDatabases,
            aspect=None,
            config={
                "manifest_s3_uri": "s3://test_bucket/prod/run_artefacts/latest/target/manifest.json",
            },
            pipeline_context=pipeline_context,
        )

        assert len(output) == 2
        assert output[0] is not None
        assert output[0].record is not None
        assert isinstance(output[0].record, MetadataChangeProposalWrapper)
        assert output[0].record.aspect is not None
        assert isinstance(output[0].record.aspect, models.ContainerClass)
