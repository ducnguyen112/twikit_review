from urllib.parse import unquote, urlparse
from pyspark.sql import SparkSession, DataFrame
from s3.utils import configure_spark_delta, create_table_from_schema, merge_schema
from typing import List, Optional
from delta.tables import DeltaTable
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window as W

def create_spark_session():
    """Create a Spark session with necessary configurations for S3"""
    builder = SparkSession.builder.master('local')
    builder = configure_spark_delta(builder)
    spark = builder.getOrCreate()
    
    return spark

def read_person_data(spark, db_uri):
    """Read data from person table"""
    r = urlparse(db_uri)
    username = unquote(r.username or '')
    password = unquote(r.password or '')
    db_df = (
        spark.read.format('jdbc').option('url', f'jdbc:{r.scheme}://{r.hostname}:{r.port}{r.path}')
        .option('driver', 'org.postgresql.Driver')
        .option('dbtable', 'person')
        .option('user', username)
        .option('password', password)
        .load()
    )
    
    return db_df


def write_to_s3(
    spark: SparkSession,
    daily_table_df: DataFrame,
    raw_table_path: str,
    primary_key: str,
    ingest_ids: Optional[List[str]] = None,
):
    daily_table_df = daily_table_df.toDF()

    if ingest_ids:
        daily_table_df = daily_table_df.filter(F.col("_ingest_id").isin(ingest_ids))

    if not DeltaTable.isDeltaTable(spark, raw_table_path):
        raw_table = create_table_from_schema(
            spark,
            daily_table_df.drop("_ingest_filename", "_ingest_at"),
            raw_table_path,
            extra_columns={
                "created_at": T.TimestampType(),
                "updated_at": T.TimestampType(),
            },
        )
    else:
        raw_table = DeltaTable.forPath(spark, raw_table_path)
        merge_schema(
            spark,
            raw_table,
            daily_table_df.drop("_ingest_filename", "_ingest_at")
            .withColumn("updated_at", F.current_timestamp())
            .withColumn("created_at", F.current_timestamp()),
        )
        raw_table = DeltaTable.forPath(spark, raw_table_path)

    w = W.partitionBy(primary_key).orderBy(
        F.desc(F.col("_ingest_at")), F.desc(F.col("_ingest_filename"))
    )
    df = daily_table_df.replace("", None)

    new_cols = []

    for column in df.columns:
        new_cols.append(F.first(column, ignorenulls=True).over(w).alias(column))
    df = (
        df.select(new_cols)
        .withColumn("row", F.row_number().over(w))
        .filter(F.col("row") == 1)
        .drop("row", "_ingest_at", "_ingest_filename")
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("created_at", F.current_timestamp())
    )

    raw_table_df = raw_table.toDF()

    missing_cols = set(raw_table_df.columns) - set(df.columns)

    df = df.withColumns({x: F.lit(None) for x in missing_cols})

    cols = {}

    for col in raw_table_df.columns:
        if col == "created_at":
            continue
        cols[col] = F.when(df[col].isNotNull(), df[col]).otherwise(raw_table_df[col])

    raw_table.merge(
        df, raw_table_df[primary_key] == df[primary_key]
    ).whenNotMatchedInsertAll().whenMatchedUpdate(
        set=cols,
    ).execute()


def main():
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Read person data
        person_df = read_person_data(spark, "postgresql://user:password@host:port/dbname")
        
        # Write to S3
        bucket_name = "your-bucket-name"  # Replace with your S3 bucket name
        file_path = "person_data/person.delta"
        write_to_s3(person_df, bucket_name, file_path)
        
        print("Successfully wrote person data to S3")
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
