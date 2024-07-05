import argparse
import logging
import requests
import pendulum
import datetime
import json
import os
import io

import pandas as pd
from google.cloud import storage, bigquery

GCS_BUCKET_ID = os.environ.get('GCS_BUCKET_ID')

logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)

class ExchangeRates():
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_currency = 'USD'

    def get_data(self, date: str) -> dict:
        """
        Getting data from Open Exchange Rates API.
        :param date: data collection date.
        :return: dict with exchange rates.
        """
        try:
            logging.info(f'Trying to get an exchange rates by {date}...')

            url = f'https://openexchangerates.org/api/historical/{date}.json?app_id={self.api_key}&base={self.base_currency}'
            result = requests.get(url)
            if not result.ok:
                raise Exception(f'API error {result.status_code}: {result.json()["description"]}')

            return result.json()['rates']
        except Exception as ex:
            logging.error(f'Unable to get data from API by {date}: {ex}')

    def process_data(self, date: str, rates: dict) -> pd.DataFrame:
        """
        Processes dict to DataFrame and adds columns.
        :param date: data collection date.
        :param rates: dict with exchange rates.
        :return: processed data.
        """
        try:
            logging.info(f'Processing data by date {date}')
            df = pd.DataFrame([(k, v) for k, v in rates.items()], columns=['currency', 'rate'])
            if (df['rate'] < 0).any():
                raise ValueError('Error value in "rates" column: value must be bigger than 0.')
            if (df['currency'].isna()).any():
                raise ValueError('Error value in "currency" column: currency must not be null.')

            df['date'] = date
            df['base'] = self.base_currency
            df['last_update'] = datetime.datetime.now()
            df = df[['date', 'base', 'currency', 'rate', 'last_update']]
            return df
        except Exception as ex:
            logging.error(f'Error processing data: {ex}')

    @staticmethod
    def load_file_to_bucket(date: str, data: pd.DataFrame, storage_client) -> None:
        """
        Upload data to GCP bucket.
        :param date: data collection date.
        :param data: df with data to upload
        :param storage_client: GCP storage client.
        :return:
        """
        try:
            bucket = storage_client.bucket(GCS_BUCKET_ID)

            string_buffer = io.StringIO()
            data.to_csv(string_buffer, index=False)

            blob_name = f'data/{date}/exchange_rates_{date}.csv'
            blob = bucket.blob(blob_name)

            blob.upload_from_string(string_buffer.getvalue())
            logging.info(f'Upload downloaded report -> {blob_name}')
        except Exception as ex:
            logging.error(f' Error loading data to bucket: {ex}')


    @staticmethod
    def from_gcs_to_biguery(date: str, storage_client, bigquery_client) -> None:
        """
        Upload data from GCP to BigQuery tables.
        :param date: data collection date.
        :param storage_client: GCP storage client.
        :param bigquery_client: BigQuery client.
        :return:
        """
        logging.info(f'Uploading file from GCS to BQ by {date}...')

        # build report schema
        bucket = storage_client.bucket(GCS_BUCKET_ID)
        schema_fields = json.loads(bucket.blob(f'schemes/exchange_rates_schema.json').download_as_string())
        schema = [bigquery.SchemaField(name=field['name'], field_type=field['type'], mode=field['mode'])
                  for field in schema_fields]

        # create load job config
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        blobs = list(storage_client.list_blobs(GCS_BUCKET_ID, prefix=f'data/{date}/'))
        date_index = date.replace('-', '')
        if len(blobs) > 0:
            # if some blobs exists, delete old table and upload all CSV files to the new one
            bq_table = f'exchange_rates.exchange_rates_{date_index}'
            bigquery_client.delete_table(bq_table, not_found_ok=True)

            for blob in blobs:
                uri = f"gs://{GCS_BUCKET_ID}/" + blob.name
                load_job = bigquery_client.load_table_from_uri(uri, bq_table, job_config=job_config)
                load_job.result()
                logging.info(f'Upload {uri} into BQ table -> {bq_table}')


def get_gcp_clients():
    """
    Creates clients instances from credential file.
    :return: storage client, bigquery client.
    """
    path = os.path.join('credentials', 'google-cloud-credentials.json')
    if os.path.exists(path):
        return storage.Client.from_service_account_json(path), bigquery.Client.from_service_account_json(path)
    else:
        raise Exception('BigQuery credentials not found. '
                        'Please, specify the environment variable "GOOGLE_PLAY_CREDENTIALS" '
                        'or put the credentials file by path "credentials/google-cloud-credentials.json"')


if __name__ == '__main__':
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument('-a', '--app_id', type=str, required=True, help='Open Exchange Rates app_id (api key).')
    args_parser.add_argument('-s', '--start_date', type=str, required=True, help='Period start date.')
    args_parser.add_argument('-e', '--end_date', type=str, required=True, help='Period end date.')
    args = args_parser.parse_args()

    end_date = pendulum.parse(args.end_date)
    start_date = pendulum.parse(args.start_date)
    dates = [date.date().isoformat() for date in pendulum.interval(start_date, end_date).range('days')]

    storage_client, bigquery_client = get_gcp_clients()
    rates_collector = ExchangeRates(api_key=args.app_id)
    for date in dates:
        rates = rates_collector.get_data(date=date)
        processed_data = rates_collector.process_data(date=date, rates=rates)
        rates_collector.load_file_to_bucket(date=date, data=processed_data, storage_client=storage_client)
        rates_collector.from_gcs_to_biguery(date=date, storage_client=storage_client, bigquery_client=bigquery_client)



