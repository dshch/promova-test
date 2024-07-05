import requests
import datetime
import os
import pandas as pd
from airflow.models.baseoperator import BaseOperator
from airflow import AirflowException


class ExchangeApiOperator(BaseOperator):
    """
    Operator gets exchange rates data from Open Exchange Rates API and saves it as csv file.
    :param api_key: api key (app id) for Open Exchange Rates API.
    :param date: data collection date.
    :param folder_path: path to folder to download data to.
    """
    template_fields = ("date", "folder_path")

    def __init__(
            self,
            api_key: str,
            date: datetime,
            folder_path: str,
            *args,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.api_key = api_key
        self.date = date
        self.base_currency = 'USD'
        self.folder_path = folder_path

    def get_data(self):
        url = (f'https://openexchangerates.org/api/historical/{self.date}.json?'
               f'app_id={self.api_key}&base={self.base_currency}')
        result = requests.get(url)

        if not result.ok:
            error_message = f'API error {result.status_code}: {result.json()["description"]}'
            raise AirflowException(error_message)

        return result

    def process_data(self, result):
        self.log.info("Process data.")
        try:
            data = result.json()['rates']
            df = pd.DataFrame([(k, v) for k, v in data.items()], columns=['currency', 'rate'])

            if (df['rate'] < 0).any():
                raise ValueError('Error value in "rates" column: value must be bigger than 0.')
            if (df['currency'].isna()).any():
                raise ValueError('Error value in "currency" column: currency must not be null.')

            df['date'] = self.date
            df['base'] = self.base_currency
            df['last_update'] = datetime.datetime.now()
            df = df[['date', 'base', 'currency', 'rate', 'last_update']]

            file_path = os.path.join(self.folder_path, f'exchange_rates_{self.date}.csv')
            df.to_csv(file_path, index=False)
            return file_path

        except Exception as ex:
            raise AirflowException(f'Error processing data: {ex}') from ex

    def execute(self, context):
        self.log.info(f'Get exchange rates by {self.date}.')
        data = self.get_data()
        file_path = self.process_data(result=data)
        return file_path
