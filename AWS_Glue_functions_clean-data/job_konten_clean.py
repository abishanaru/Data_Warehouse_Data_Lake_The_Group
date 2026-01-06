import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import ApplyMapping, SelectFromCollection
from awsglue.utils import getResolvedOptions
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.context import SparkContext

import gs_flatten


# --- Configuration ---
S3_INPUT_PATH = "s3://zurichcantonexpenditures/raw_rpk_api/konten/konten.json"
S3_OUTPUT_PATH = "s3://zurichcantonexpenditures/clean/RPK_Konten_clean/"

DATA_QUALITY_RULESET = """
Rules = [
  ColumnCount > 0,
  RowCount > 0,

  Completeness "id" >= 0.99,
  ColumnValues "id" >= 0,

  Completeness "kontoNr" >= 0.99,
  DistinctValuesCount "kontoNr" > 0,

  Completeness "`institution.key`" >= 0.95,
  Completeness "`departement.key`" >= 0.90
]
"""


def _create_glue_job():
    """
    Create Spark/Glue contexts and initialize the Glue job.
    """
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    spark_context = SparkContext()
    glue_context = GlueContext(spark_context)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    return glue_context, job


def main():
    glue_context, job = _create_glue_job()

    # Read JSON from S3 (RPK API)
    source_dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        format="json",
        format_options={"jsonPath": "$.value[*]", "multiLine": "true"},
        connection_options={"paths": [S3_INPUT_PATH], "recurse": True},
        transformation_ctx="AmazonS3_node1764489915096",
    )

    # Flatten nested structures
    flattened_dyf = source_dyf.gs_flatten()

    # Apply mapping (schema)
    mapped_dyf = ApplyMapping.apply(
        frame=flattened_dyf,
        mappings=[
            ("bezeichnung", "string", "bezeichnung", "string"),
            ("id", "int", "id", "int"),
            ("`institution.bezeichnung`", "string", "`institution.bezeichnung`", "string"),
            ("`institution.departement.bezeichnung`", "string", "`departement.bezeichnung`", "string"),
            ("`institution.departement.key`", "string", "`departement.key`", "string"),
            ("`institution.departement.kurzname`", "string", "`departement.kurzname`", "string"),
            ("`institution.key`", "string", "`institution.key`", "string"),
            ("`institution.kurzname`", "string", "`institution.kurzname`", "string"),
            ("kontoNr", "string", "kontoNr", "string"),
        ],
        transformation_ctx="ChangeSchema_node1764490421457",
    )

    # Data quality checks
    dq_result = EvaluateDataQuality().process_rows(
        frame=mapped_dyf,
        ruleset=DATA_QUALITY_RULESET,
        publishing_options={
            "dataQualityEvaluationContext": "EvaluateDataQuality_node1764489892847",
            "enableDataQualityResultsPublishing": True,
        },
        additional_options={
            "performanceTuning.caching": "CACHE_NOTHING",
            "observations.scope": "NONE",
        },
    )

    # Keep only the original data output
    original_data = SelectFromCollection.apply(
        dfc=dq_result,
        key="originalData",
        transformation_ctx="originalData_node1767709533275",
    )

    # Write parquet
    glue_context.write_dynamic_frame.from_options(
        frame=original_data,
        connection_type="s3",
        format="glueparquet",
        connection_options={"path": S3_OUTPUT_PATH, "partitionKeys": []},
        format_options={"compression": "snappy"},
        transformation_ctx="RPK_Konten_clean_node1764492117658",
    )

    job.commit()