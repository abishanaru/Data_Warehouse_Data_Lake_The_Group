import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import ApplyMapping
from awsglue.utils import getResolvedOptions
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.context import SparkContext

import gs_flatten


# --- Configuration ---
S3_INPUT_PATH = "s3://zurichcantonexpenditures/raw_rpk_api/institutionen/"
S3_OUTPUT_PATH = "s3://zurichcantonexpenditures/clean/RPK_Institutionen/"
CATALOG_DATABASE = "datalake_clean"
CATALOG_TABLE = "RPK_Institutionen_clean"

DATA_QUALITY_RULESET = """
Rules = [
  ColumnCount > 0,
  RowCount > 0,

  Completeness "key" >= 0.99,
  DistinctValuesCount "key" > 0,

  Completeness "bezeichnung" >= 0.95,

  Completeness "`departement.key`" >= 0.90,
  Completeness "`departement.bezeichnung`" >= 0.90
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

    # Read JSON from S3 (RPK API)
    source_dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        format="json",
        format_options={"jsonPath": "$.value[*]", "multiLine": "true"},
        connection_options={"paths": [S3_INPUT_PATH], "recurse": True},
        transformation_ctx="AmazonS3_node1764273990703",
    )

    # Flatten nested structures
    flattened_dyf = source_dyf.gs_flatten()

    # Apply mapping (schema)
    mapped_dyf = ApplyMapping.apply(
        frame=flattened_dyf,
        mappings=[
            ("bezeichnung", "string", "bezeichnung", "string"),
            ("`departement.bezeichnung`", "string", "`departement.bezeichnung`", "string"),
            ("`departement.key`", "string", "`departement.key`", "string"),
            ("`departement.kurzname`", "string", "`departement.kurzname`", "string"),
            ("key", "string", "key", "string"),
            ("kurzname", "string", "kurzname", "string"),
        ],
        transformation_ctx="ChangeSchema_node1764274866010",
    )

    # Data quality checks
    EvaluateDataQuality().process_rows(
        frame=mapped_dyf,
        ruleset=DATA_QUALITY_RULESET,
        publishing_options={
            "dataQualityEvaluationContext": "EvaluateDataQuality_node1764487602346",
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
        transformation_ctx="AmazonS3_node1764489270886",
    )
    sink.setCatalogInfo(
        catalogDatabase=CATALOG_DATABASE,
        catalogTableName=CATALOG_TABLE,
    )
    sink.setFormat("glueparquet", compression="snappy")
    sink.writeFrame(mapped_dyf)

    job.commit()