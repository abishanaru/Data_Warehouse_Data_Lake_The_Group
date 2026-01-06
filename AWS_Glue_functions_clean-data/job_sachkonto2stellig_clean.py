import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import ApplyMapping, SelectFromCollection
from awsglue.utils import getResolvedOptions
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.context import SparkContext


# --- Configuration ---
S3_INPUT_PATH = "s3://zurichcantonexpenditures/raw_rpk_api/sachkonto2stellig/"
S3_OUTPUT_PATH = "s3://zurichcantonexpenditures/clean/sachkonto2stellig/"

DATA_QUALITY_RULESET = """
Rules = [
  ColumnCount > 0,
  RowCount > 0,

  Completeness "jahr" >= 0.99,
  ColumnValues "jahr" >= 1900,
  ColumnValues "jahr" <= 2100,

  Completeness "sachkonto" >= 0.99,
  DistinctValuesCount "sachkonto" > 0,

  Completeness "institution" >= 0.90,
  Completeness "betragstyp" >= 0.90,

  # amount can be missing in edge cases; keep slightly lower but still strict
  Completeness "betrag" >= 0.95
]
"""


def _create_glue_job():
    """Create Spark/Glue contexts and initialize the Glue job."""
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    spark_context = SparkContext()
    glue_context = GlueContext(spark_context)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    return glue_context, job


def main():
    glue_context, job = _create_glue_job()

    # Read JSON from S3
    source_dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        format="json",
        format_options={"jsonPath": "$.value[*]", "multiLine": "true"},
        connection_options={"paths": [S3_INPUT_PATH], "recurse": True},
        transformation_ctx="AmazonS3_node1764094569776",
    )

    # Apply mapping (schema)
    mapped_dyf = ApplyMapping.apply(
        frame=source_dyf,
        mappings=[
            ("betrag", "string", "betrag", "double"),
            ("betragstyp", "string", "betragstyp", "string"),
            ("institution", "string", "institution", "string"),
            ("jahr", "int", "jahr", "int"),
            ("sachkonto", "string", "sachkonto", "string"),
        ],
        transformation_ctx="ChangeSchema_node1764259516535",
    )

    # Data quality checks
    dq_result = EvaluateDataQuality().process_rows(
        frame=mapped_dyf,
        ruleset=DATA_QUALITY_RULESET,
        publishing_options={
            "dataQualityEvaluationContext": "EvaluateDataQuality_node1764259038358",
            "enableDataQualityResultsPublishing": True,
        },
        additional_options={
            "observations.scope": "NONE",
            "performanceTuning.caching": "CACHE_NOTHING",
        },
    )

    # Keep only original data
    original_data = SelectFromCollection.apply(
        dfc=dq_result,
        key="originalData",
        transformation_ctx="originalData_node1767716593154",
    )

    # Write parquet
    glue_context.write_dynamic_frame.from_options(
        frame=original_data,
        connection_type="s3",
        format="glueparquet",
        connection_options={"path": S3_OUTPUT_PATH, "partitionKeys": []},
        format_options={"compression": "snappy"},
        transformation_ctx="S3_Parquet_Sachkonto2Stellig_node1764259810307",
    )

    job.commit()