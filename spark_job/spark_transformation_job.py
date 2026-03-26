
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,count,avg,when,lit,expr
import logging
import sys

#intalize the logging 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s -%(message)s"
)

logger = logging.getLogger(__name__)

def main(env,bq_project,bq_dataset,transformed_table,route_insights_table,origin_insights_table):
    try:
        #inilitze Sparksession
        spark = SparkSession.builder.appName("FlightBookingAnalysis").config("spark.sql.catologImplementation","hive").getOrCreate()

        logger.info("Spark Session is intialized")

        #resolve GCS path based on the enviroment
        input_path = f"gs://airflow-projects-bucket-kd/flight-booking-analysis/source-{env}"
        logger.info(f"Input File Path resolve : {input_path}")

        #read the data from gcs
        data = spark.read.csv(input_path,header=True,inferSchema=True)
        logger.info("Data Read from gcs")

        #data Transformation
        transformed_data = data.withColumn(
            'is_weekend',when(col("flight_day").isin("Sat","Sun"),lit(1)).otherwise(lit(0))
        ).withColumn(
            "lead_time_category",when(col("purchase_lead") < 7,lit("Last-Minute"))
                                 .when((col("purchase_lead") >=7) & (col("purchase_lead")< 30),lit("Short-term")) 
                                 .otherwise(lit("Long-term"))  
        ).withColumn(
            "booking_success_rate",expr("booking_complete/num_passengers")
        )

        #Aggrefations for insights
        route_insights = transformed_data.groupBy("route").agg(
            count("*").alias("total_bookiungs"),
            avg("flight_duration").alias("avg_flight_duration"),
            avg("lenth_of_stay").alias("avg_stay_length")
        )

        booking_origin_insights = transformed_data.groupBy("booking_origin").agg(
            count("*").alias("total_bookings"),
            avg("booking_success_rate").alias("success_rate"),
            avg("purchase_lead").alias("avg_purchase_lead")   
        )

        logger.info("Data transformations completed")

        #write transformed data to bigquery
        logger.info(f"writing transformed data to bigquery table : {bq_project}:{bq_dataset}.{route_insights_table}")
        route_insights.write.format("bigquery").option("table",f"{bq_project}:{bq_dataset}.{route_insights_table}").option("writeMethod","direct").mode("overwrite").save()

        logger.info(f"writing transformed data to bigquery table : {bq_project}:{bq_dataset}.{origin_insights_table}")
        route_insights.write.format("bigquery").option("table",f"{bq_project}:{bq_dataset}.{origin_insights_table}").option("writeMethod","direct").mode("overwrite").save()
        
        logger.info("Data writen to bigquery successfully")
    
    except Exception as e:
        logger.error(f"An error occurred : {e}")
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    #Parse command-line arguments
    parser = argparse.ArgumentParser(description="Process flight booking data and write to bigquery")
    parser.add_argument("--env",required=True,help="Enviroment (e.g,dev,prod)")
    parser.add_argument("--bq_project",required=True,help="Biqquery project ID")
    parser.add_argument("--bq_dataset",required=True,help="Bigquery dataset name")
    parser.add_argument("--transformed_table",required=True,help="Bigquery table for transformed data")
    parser.add_argument("--route_insights_table",required=True,help="Bigquery table for route_insights_table")
    parser.add_argument("--origin_insights_table",required=True,help="Bigquery table for origin_insights_table")
    
    args = parser.parse_args()

    #call the main functions with parsed arguments
    main(
        env=args.env,
        bq_project=args.bq_project,
        bq_dataset=args.bq_dataset,
        transformed_table=args.transformed_table,
        route_insights_table=args.route_insights_table,
        origin_insights_table=args.origin_insights_table,  
    )
