from datetime import datetime,timedelta
import uuid
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.models import Variable


#dag default arguments
default_args = {
    'owner' : 'kiran',
    'depends_on_past' : False,
    'retries' : 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026,3,25)
}

#define the dag
with DAG(
    dag_id = 'flight_booking_dataproc_bq_dag',
    default_args = default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    
    #fetch enviroment varibles
    env = Variable.get('env',default_var='dev')
    gcs_bucket = Variable.get('gcs_bucket',default_var='airflow-projects-bucket')
    bq_project = Variable.get('bq_project',default_var='project-b33d6da0-bcfc-471f-99c')
    bq_dataset = Variable.get('bq_dataset',default_var=f'flight_data_{env}')
    tables = Variable.get('tables',deserialize_json=True)

    #extract tables names from the tables variables
    transformed_table = tables["transformed_table"]
    route_insights_table = tables["route_insights_table"]
    origin_insights_table = tables["origin_insights_table"]

    #generate a unique batch id using UUID
    batch_id = f"flight-booking-batch-{env}-{str(uuid.uuid4())[:8]}"

    #task 1 : file sensor for GCS
    file_sensor = GCSObjectExistenceSensor(
        task_id = "check_file_arrival",
        bucket=gcs_bucket,
        object=f"flight-booking-analysis/source-{env}/flight_booking.csv",
        google_cloud_conn_id = "google_cloud_default",
        timeout = 300,
        poke_interval=30, #time between checks,
        mode="poke"
    )

    #task 2 : Submit pyspark jobn to dataproc serverless,
    batch_details = {
        "pyspark_batch" : {
            "main_python_file_uri": f"gs://{gcs_bucket}/flight-booking-analysis/spark-job/spark_transformation_job.py",
            "python_file_uris": [],
            "jar_file_uris": [],
            "args": [
                f"--env={env}",
                f"--bq_project={bq_project}",
                f"--bq_dataset={bq_dataset}",
                f"--transformed_table={transformed_table}",
                f"--route_insights_table={route_insights_table}",
                f"--origin_insights_table={origin_insights_table}"
            ]
        },
        "runtime_config" : {
            "version": "2.2",
        },
        "enviroment_config":{
            "execution_config": {
                "service_account": "336632331058-compute@developer.gserviceaccount.com",
                "network_uri": "projects/project-b33d6da0-bcfc-471f-99c/global/networks/default",
                "subnetwork_uri": "projects/project-b33d6da0-bcfc-471f-99c/regions/us-central1/subnetworks/default"
            }
        },
    }

    pyspark_task = DataprocCreateBatchOperator(
        task_id = 'run_spark_job_on_dataproc_serverless',
        batch=batch_details,
        batch_id=batch_id,
        project_id='project-b33d6da0-bcfc-471f-99c',
        region="us-central1",
        gcp_conn_id="google_cloud_default"
    )

    #task dependencies
    file_sensor >> pyspark_task