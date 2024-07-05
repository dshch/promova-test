from datetime import datetime
import pendulum
import yaml

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from include.exchange_api_operator import ExchangeApiOperator


with DAG(
        "exchange_rates",
        schedule_interval="0 5 * * *",
        start_date=pendulum.datetime(2024, 7, 4, tz="Europe/Kiev"),
        catchup=False,
        tags=["etl", "exchange_rates"],
        doc_md="""
        ### Exchange rates
        This DAG downloads exchange rates data from Open Exchange Rates API and
        uploads it into `GCP` bucket and transfer updates to `BigQuery` main table.
        """
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")


    @task
    def create_folder():
        import os

        reports_folder_path = f'/usr/local/airflow/data/exchange_rates/{datetime.now().isoformat()}'
        if not os.path.exists(reports_folder_path):
            os.makedirs(reports_folder_path)

        return reports_folder_path

    @task
    def clear_report_folder(ti):
        import shutil
        folder_path = ti.xcom_pull(task_ids='create_folder')
        shutil.rmtree(folder_path, ignore_errors=True)


    with open('/usr/local/airflow/dags/configs/exchange_rates.yml', 'r') as file:
        config = yaml.safe_load(file)

        project_id = config['bq_etl']['project_id']
        location = config['bq_etl']['location']
        bucket_id = config['bucket_id']
        api_key = config['api_key']

        folder_path = create_folder()

        report_path = ExchangeApiOperator(
            api_key=api_key,
            date='{{ ds }}',
            folder_path=folder_path,
            task_id='collect_data'
        )
        upload_data_to_gcs = LocalFilesystemToGCSOperator(
            task_id="upload_data_to_gcs",
            gcp_conn_id='google_cloud_default',
            dst='data/{{ ds }}/',
            src="{{ ti.xcom_pull(task_ids='collect_data', key='return_value') }}",
            bucket=bucket_id,
            mime_type='csv',
        )
        load_from_gcp_to_bq = GCSToBigQueryOperator(
            task_id="load_from_gcp_to_bq",
            bucket=bucket_id,
            source_objects='data/{{ ds }}/exchange_rates_{{ ds }}.csv',
            destination_project_dataset_table='exchange_rates.exchange_rates_{{ ds }}',
            schema_object='schemes/exchange_rates_schema.json',
            source_format="CSV",
            skip_leading_rows=1,
            write_disposition="WRITE_TRUNCATE",
        )
        clear_folder = clear_report_folder()

        start >> folder_path >> report_path >> upload_data_to_gcs >> load_from_gcp_to_bq >> clear_folder >> end
