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
S3_INPUT_PATHS = [
    "s3://zurichcantonexpenditures/processed_paris_api/abstimmungen_alle.json"
]

S3_OUTPUT_PATH = "s3://zurichcantonexpenditures/clean/paris_abstimmungen/"
CATALOG_DATABASE = "datalake_clean"
CATALOG_TABLE = "paris_abstimmungen_clean"

# Ruleset for quality check
DATA_QUALITY_RULESET = """
Rules = [
  ColumnCount > 0,
  RowCount > 0,

  Completeness "geschaeftsnummer" >= 0.99,
  DistinctValuesCount "geschaeftsnummer" > 0,

  # stimm_datum can be missing for some records; keep threshold a bit lower.
  Completeness "stimm_datum" >= 0.60,

  Completeness "year" >= 0.95,
  ColumnValues "year" >= 1900,
  ColumnValues "year" <= 2100,

  ColumnValues "ja" >= 0,
  ColumnValues "nein" >= 0,
  ColumnValues "enthalten" >= 0,
  ColumnValues "nicht_praesent" >= 0,
  ColumnValues "total_stimmen" >= 0
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


def _extract_year_from_geschaeftsnummer(dyf):
    """
    Derive year from geschaeftsnummer.
    """
    return dyf.gs_derived(
        colName="year",
        expr="cast(substring(geschaeftsnummer, 1, 4) as int)",
    )


def main():
    glue_context, job = _create_glue_job()

    # Read JSON from S3
    source_dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        format="json",
        format_options={"multiLine": "false"},
        connection_options={"paths": S3_INPUT_PATHS, "recurse": True},
        transformation_ctx="AmazonS3_node1765397437675",
    )

    # Apply mapping (schema normalization)
    mapped_dyf = ApplyMapping.apply(
        frame=source_dyf,
        mappings=[
            ("dateiname", "string", "dateiname", "string"),
            ("geschäftstitel", "string", "geschaeftstitel", "string"),
            ("geschäftsnummer", "string", "geschaeftsnummer", "string"),
            ("stimm_datum", "string", "stimm_datum", "string"),
            ("stimm_zeit", "string", "stimm_zeit", "string"),
            ("ja", "int", "ja", "int"),
            ("nein", "int", "nein", "int"),
            ("enthalten", "int", "enthalten", "int"),
            ("nicht_praesent", "int", "nicht_praesent", "int"),
            ("total_stimmen", "int", "total_stimmen", "int"),
            ("stichentscheid", "string", "stichentscheid", "string"),
        ],
        transformation_ctx="ChangeSchema_node1765398871685",
    )

    # Derived column: year
    with_year_dyf = _extract_year_from_geschaeftsnummer(mapped_dyf)

    # Filter out empty business numbers
    filtered_dyf = Filter.apply(
        frame=with_year_dyf,
        f=lambda row: bool(re.match(".+", row["geschaeftsnummer"])),
        transformation_ctx="Filter_node1765400336220",
    )

    # Data quality checks
    EvaluateDataQuality().process_rows(
        frame=filtered_dyf,
        ruleset=DATA_QUALITY_RULESET,
        publishing_options={
            "dataQualityEvaluationContext": "EvaluateDataQuality_node1765396655448",
            "enableDataQualityResultsPublishing": True,
        },
        additional_options={
            "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
            "observations.scope": "ALL",
        },
    )

    # Write parquet + update Data Catalog table
    sink = glue_context.getSink(
        path=S3_OUTPUT_PATH,
        connection_type="s3",
        updateBehavior="LOG",
        partitionKeys=[],
        enableUpdateCatalog=True,
        transformation_ctx="AmazonS3_node1765399865077",
    )
    sink.setCatalogInfo(
        catalogDatabase=CATALOG_DATABASE,
        catalogTableName=CATALOG_TABLE,
    )
    sink.setFormat("glueparquet", compression="snappy")
    sink.writeFrame(filtered_dyf)

    job.commit()
