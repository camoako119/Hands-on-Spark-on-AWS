import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import coalesce, col, lit, to_date, upper


# -----------------------------------------------------------------------------
# IMPORTANT: Replace these two bucket names with the exact names you create.
# Bucket names must be globally unique.
# -----------------------------------------------------------------------------
LANDING_BUCKET = "reviews-landing-ca"
PROCESSED_BUCKET = "reviews-processed-ca"

INPUT_PATH = f"s3://{LANDING_BUCKET}/reviews.csv"
PROCESSED_DATA_PATH = f"s3://{PROCESSED_BUCKET}/processed-data/"
ANALYTICS_BASE_PATH = f"s3://{PROCESSED_BUCKET}/Athena Results/"


# Initialize the AWS Glue job and Spark session.
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)


# Read the CSV from the landing bucket.
# multiLine=True is required because some review_text values span two lines.
raw_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("multiLine", True)
    .option("quote", '"')
    .option("escape", '"')
    .csv(INPUT_PATH)
)


# Clean and transform the dataset.
# The source CSV contains one malformed row whose review_id is "auto_comment".
# Casting review_id to integer and requiring the cast to be non-null removes it.
transformed_df = (
    raw_df
    .filter(col("review_id").cast("integer").isNotNull())
    .filter(
        col("product_id").isNotNull()
        & col("customer_id").isNotNull()
        & col("review_date").isNotNull()
    )
    .withColumn("review_id", col("review_id").cast("integer"))
    .withColumn("rating", coalesce(col("rating").cast("integer"), lit(0)))
    .withColumn("review_date", to_date(col("review_date"), "yyyy-MM-dd"))
    .withColumn("review_text", coalesce(col("review_text"), lit("No review text")))
    .withColumn("product_id_upper", upper(col("product_id")))
)

print(f"Valid transformed row count: {transformed_df.count()}")


# Save the complete cleaned dataset as Parquet.
# Parquet is columnar and is efficient for later analytics.
(
    transformed_df
    .write
    .mode("overwrite")
    .parquet(PROCESSED_DATA_PATH)
)


# Register a temporary Spark SQL view.
transformed_df.createOrReplaceTempView("product_reviews")


# Query 1: Average rating and review count for each product.
product_average_ratings_df = spark.sql("""
    SELECT
        product_id_upper,
        ROUND(AVG(rating), 2) AS average_rating,
        COUNT(*) AS review_count
    FROM product_reviews
    GROUP BY product_id_upper
    ORDER BY average_rating DESC, product_id_upper ASC
""")


# Query 2: Total number of reviews submitted on each date.
daily_review_counts_df = spark.sql("""
    SELECT
        review_date,
        COUNT(*) AS review_count
    FROM product_reviews
    GROUP BY review_date
    ORDER BY review_date ASC
""")


# Query 3: Five customers who submitted the most reviews.
# customer_id is included as a secondary sort to make ties deterministic.
top_5_customers_df = spark.sql("""
    SELECT
        customer_id,
        COUNT(*) AS review_count
    FROM product_reviews
    GROUP BY customer_id
    ORDER BY review_count DESC, customer_id ASC
    LIMIT 5
""")


# Query 4: Number of reviews for every rating value.
# Rating 0 represents source rows whose rating was missing.
rating_distribution_df = spark.sql("""
    SELECT
        rating,
        COUNT(*) AS review_count
    FROM product_reviews
    GROUP BY rating
    ORDER BY rating ASC
""")


def write_single_csv(dataframe, output_path):
    """Write a small query result as one headered CSV part file."""
    (
        dataframe
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(output_path)
    )
    print(f"Wrote query output to {output_path}")


# Write each result to a separate S3 folder.
write_single_csv(
    product_average_ratings_df,
    f"{ANALYTICS_BASE_PATH}product_average_ratings/",
)
write_single_csv(
    daily_review_counts_df,
    f"{ANALYTICS_BASE_PATH}daily_review_counts/",
)
write_single_csv(
    top_5_customers_df,
    f"{ANALYTICS_BASE_PATH}top_5_customers/",
)
write_single_csv(
    rating_distribution_df,
    f"{ANALYTICS_BASE_PATH}rating_distribution/",
)


# Mark the AWS Glue job as successfully completed.
job.commit()
