import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, when, lit, expr
import logging
import sys

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def main(env, bq_project, bq_dataset, transformed_table, route_insights_table, origin_insights_table):
    try:
        # Initialize Spark session
        spark = SparkSession.builder.appName("FlightBookingAnalysis").getOrCreate()
        logger.info("Spark Session initialized")

        # Resolve GCS path based on environment
        input_path = f"gs://airflow-projects-bucket-kd/flight-booking-analysis/source-{env}"
        logger.info(f"Input File Path: {input_path}")

        # Read data from GCS
        data = spark.read.csv(input_path, header=True, inferSchema=True)
        logger.info("Data read from GCS")

        # Data transformations
        transformed_data = data.withColumn(
            'is_weekend', when(col("flight_day").isin("Sat","Sun"), lit(1)).otherwise(lit(0))
        ).withColumn(
            "lead_time_category",
            when(col("purchase_lead") < 7, lit("Last-Minute"))
            .when((col("purchase_lead") >=7) & (col("purchase_lead") < 30), lit("Short-term"))
            .otherwise(lit("Long-term"))
        ).withColumn(
            "booking_success_rate", expr("booking_complete / num_passengers")
        )

        # Aggregations for insights
        route_insights = transformed_data.groupBy("route").agg(
            count("*").alias("total_bookings"),
            avg("flight_duration").alias("avg_flight_duration"),
            avg("length_of_stay").alias("avg_stay_length")
        )

        origin_insights = transformed_data.groupBy("booking_origin").agg(
            count("*").alias("total_bookings"),
            avg("booking_success_rate").alias("success_rate"),
            avg("purchase_lead").alias("avg_purchase_lead")
        )

        logger.info("Data transformations completed")

        # Write transformed data to BigQuery
        logger.info(f"Writing route insights to BigQuery: {bq_project}.{bq_dataset}.{route_insights_table}")
        route_insights.write.format("bigquery") \
            .option("table", f"{bq_project}.{bq_dataset}.{route_insights_table}") \
            .option("writeMethod", "direct") \
            .option("temporaryGcsBucket", "airflow-projects-bucket-kd") \
            .option("allowFieldAddition", "true") \
            .mode("append") \
            .save()

        logger.info(f"Writing origin insights to BigQuery: {bq_project}.{bq_dataset}.{origin_insights_table}")
        origin_insights.write.format("bigquery") \
            .option("table", f"{bq_project}.{bq_dataset}.{origin_insights_table}") \
            .option("writeMethod", "direct") \
            .option("temporaryGcsBucket", "airflow-projects-bucket-kd") \
            .option("allowFieldAddition", "true") \
            .mode("append") \
            .save()

        logger.info("Data successfully written to BigQuery")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process flight booking data and write to BigQuery")
    parser.add_argument("--env", required=True, help="Environment (dev/prod)")
    parser.add_argument("--bq_project", required=True, help="BigQuery project ID")
    parser.add_argument("--bq_dataset", required=True, help="BigQuery dataset name")
    parser.add_argument("--transformed_table", required=True, help="Transformed table name")
    parser.add_argument("--route_insights_table", required=True, help="Route insights table name")
    parser.add_argument("--origin_insights_table", required=True, help="Origin insights table name")
    
    args = parser.parse_args()

    main(
        env=args.env,
        bq_project=args.bq_project,
        bq_dataset=args.bq_dataset,
        transformed_table=args.transformed_table,
        route_insights_table=args.route_insights_table,
        origin_insights_table=args.origin_insights_table
    )