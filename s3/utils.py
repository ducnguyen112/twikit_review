from pyspark.sql import DataFrame, SparkSession
from dataclasses import dataclass
from typing import Dict
from delta.tables import DeltaTable
from pyspark.sql.types import DataType


@dataclass
class GlobalOptions:
    master_url: str
    conf: Dict[str, str]


global_options: GlobalOptions = GlobalOptions("", {})


def configure_spark_delta(builder: SparkSession.Builder) -> SparkSession.Builder:
    builder = (
        builder.config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.hadoop.fs.s3a.endpoint", "salesbox-storage.cluster.theinfi.tech")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", "2TT4v1sK6abSvIcJKTmx")
        .config(
            "spark.hadoop.fs.s3a.secret.key", "7EjO4Q7ZGYy6ouiGDrpxmBY3CzrTMUxJBnT6bhMH"
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.driver.maxResultSize",
            "16G",
        )
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.0.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.13.1,org.apache.hadoop:hadoop-aws:3.2.4,org.postgresql:postgresql:42.6.0",
        )
        .config(
            "spark.jars.repositories",
            "https://repo1.maven.org/maven2",
        )
        .config(
            "spark.driver.extraJavaOptions",
            "-Xss4M",
        )
        .config(
            "spark.executor.extraJavaOptions",
            "-Xss4M",
        )
        .config(
            "spark.python.profile.memory",
            "true"
        )
        .config(
            "spark.sql.execution.arrow.pyspark.selfDestruct.enabled",
            "true"
        )
        .config(
            "spark.sql.execution.arrow.pyspark.enabled",
            "true"
        )
        .config(
            "spark.sql.execution.pythonUDF.arrow.enabled",
            "true"
        )
    )

    for k, v in global_options.conf.items():
        builder = builder.config(k, v)
    return builder

def create_table_from_schema(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    extra_columns: Dict[str, DataType] = {},
):
    schema = df.schema
    for field in schema.fields:
        field.nullable = True
    for k, v in extra_columns.items():
        schema.add(k, v)

    return (
        DeltaTable.createIfNotExists(spark).location(path).addColumns(schema).execute()
    )

def merge_schema(spark: SparkSession, table: DeltaTable, df: DataFrame):
    dest = table.toDF()
    table_detail = table.detail().collect()[0]
    missing_columns = set(df.columns).difference(dest.columns)
    if missing_columns:
        col_defs = []
        for col in missing_columns:
            col_defs.append(f"{col} {df.schema[col].dataType.simpleString()}")
        sql = f"ALTER TABLE delta.`{table_detail.location}` ADD COLUMNS ({','.join(col_defs)})"
        spark.sql(sql)