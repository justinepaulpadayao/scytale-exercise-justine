"""
This module performs data transformation on repository and pull request data using PySpark.
It reads JSON data, processes and aggregates it according to specified business logic, and
saves the result as a Parquet file.
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, lower

# Define paths
PROJECT_ROOT_PATH = Path(__file__).resolve().parents[4] / "data"
INPUT_DIRECTORY = PROJECT_ROOT_PATH / "input"
OUTPUT_DIRECTORY = PROJECT_ROOT_PATH / "output"

REPO_DATA_PATH = INPUT_DIRECTORY / "scytale_exercise_repositories.json"
PR_DATA_PATH = INPUT_DIRECTORY / "scytale_exercise_pull_requests.json"

# Initialize Spark Session
spark = SparkSession.builder.appName("scytale_data_processing").getOrCreate()

# Read repository data and select relevant columns
repo_raw_df = spark.read.json(str(REPO_DATA_PATH), multiLine=True)
repo_filtered_df = repo_raw_df.select(
    split(col("full_name"), "/").getItem(0).alias("organization_name"),
    col("id").alias("repository_id"),
    col("name").alias("repository_name"),
    col("owner.login").alias("repository_owner"),
)

# Read PR data and aggregate metrics
pr_raw_df = spark.read.json(str(PR_DATA_PATH), multiLine=True)
pr_raw_df.createOrReplaceTempView("pull_requests")
pr_aggregated_df = spark.sql(
    """
    SELECT
        head.repo.id AS repository_id,
        COUNT(*) AS num_prs,
        SUM(CASE WHEN merged_at IS NOT NULL THEN 1 ELSE 0 END) AS num_prs_merged,
        MAX(merged_at) AS merged_at
    FROM pull_requests
    GROUP BY head.repo.id
    """
)

joined_df = pr_aggregated_df.join(repo_filtered_df, "repository_id")

final_df = joined_df.withColumn(
    "is_compliant",
    when(
        (col("num_prs") == col("num_prs_merged"))
        & lower(col("repository_owner")).contains("scytale"),
        True,
    ).otherwise(False),
)

final_df = final_df.select(
    col("organization_name"),
    col("repository_id"),
    col("repository_name"),
    col("repository_owner"),
    col("num_prs"),
    col("num_prs_merged"),
    col("merged_at"),
    col("is_compliant"),
)

# Save the final DataFrame as parquet file
final_df.write.partitionBy("repository_name").mode("overwrite").parquet(
    str(OUTPUT_DIRECTORY)
)
