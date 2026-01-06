"""
AWS Glue Job: PARIS Geschaefte (clean)

Reads processed PARIS JSON files, extracts/cleans numeric fields, standardizes the
schema, runs data quality checks, and writes parquet to the clean zone.
"""

import re
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import ApplyMapping, Filter
from awsglue.utils import getResolvedOptions
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.context import SparkContext

import gs_derived


# --- Configuration ---
S3_INPUT_PATH = "s3://zurichcantonexpenditures/processed_paris_api/geschaefte.json"
S3_OUTPUT_PATH = "s3://zurichcantonexpenditures/clean/paris_geschaefte/"
CATALOG_DATABASE = "datalake_clean"
CATALOG_TABLE = "paris_geschaefte_clean"

DATA_QUALITY_RULESET = """
Rules = [
  ColumnCount > 0,
  RowCount > 0,

  Completeness "grnr" >= 0.98,
  DistinctValuesCount "grnr" > 0,

  Completeness "geschaeftsart" >= 0.90,

  # year derived from grnr: expect realistic values
  Completeness "year" >= 0.95,
  ColumnValues "year" >= 1900,
  ColumnValues "year" <= 2100
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


def _extract_year(dyf):
    """
    Extract year from grnr (first 4 digits) and cast to int for range checks.
    """
    return dyf.gs_derived(
        colName="year",
        expr="cast(substring(grnr, 1, 4) as int)",
    )


def main():
    glue_context, job = _create_glue_job()
    spark = glue_context.spark_session

    # Read source JSON
    source_dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        format="json",
        format_options={"multiLine": "false"},
        connection_options={"paths": [S3_INPUT_PATH], "recurse": True},
        transformation_ctx="AmazonS3_node1764505300943",
    )

    # Convert a nested array field into a comma-separated string
    df = source_dyf.toDF()
    df = df.withColumn(
        "geschaeftsart",
        df["geschaeftsart"].cast("string"),
    )
    # Normalize to a clean list.
    df = df.withColumn(
        "geschaeftsart",
        df["geschaeftsart"]
        .cast("string")
        .replace("[", "")
        .replace("]", "")
        .replace('"', ""),
    )

    normalized_dyf = glue_context.create_dynamic_frame.fromDF(
        df,
        transformation_ctx="SQLTransform_node1764505485959",
    )

    # Schema mapping
    mapped_dyf = ApplyMapping.apply(
        frame=normalized_dyf,
        mappings=[
            ("dateiname", "string", "dateiname", "string"),
            ("grnr", "string", "grnr", "string"),
            ("titel", "string", "titel", "string"),
            ("pdf_files", "string", "pdf_files", "string"),
            ("geschaeftsart", "string", "geschaeftsart", "string"),
        ],
        transformation_ctx="ChangeSchema_node1764505556042",
    )

    # Derived year
    with_year_dyf = _extract_year(mapped_dyf)

    # Filter out empty grnr
    filtered_dyf = Filter.apply(
        frame=with_year_dyf,
        f=lambda row: bool(re.match(".+", row["grnr"])),
        transformation_ctx="Filter_node1764505672970",
    )

    # Data quality checks
    EvaluateDataQuality().process_rows(
        frame=filtered_dyf,
        ruleset=DATA_QUALITY_RULESET,
        publishing_options={
            "dataQualityEvaluationContext": "EvaluateDataQuality_node1764505690986",
            "enableDataQualityResultsPublishing": True,
        },
        additional_options={
            "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
            "observations.scope": "ALL",
        },
    )

    # Write parquet + update catalog
    sink = glue_context.getSink(
        path=S3_OUTPUT_PATH,
        connection_type="s3",
        updateBehavior="LOG",
        partitionKeys=[],
        enableUpdateCatalog=True,
        transformation_ctx="AmazonS3_node1764505727294",
    )
    sink.setCatalogInfo(
        catalogDatabase=CATALOG_DATABASE,
        catalogTableName=CATALOG_TABLE,
    )
    sink.setFormat("glueparquet", compression="snappy")
    sink.writeFrame(filtered_dyf)

    job.commit()

