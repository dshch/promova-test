## Python Script

Collects data for multiple dates.
```bash
$ python exchange_rates.py --start_date <YYYY-mm-dd> --end_date <YYYY-mm-dd> --app_id <app_id>
```
Where:
* `--app_id` - OpenExchangeRates App Id (API key).
* `--start_date` - Starting collection date,
* `--end_date` - Ending collecton date,

### Credentials Required

<pre>
<span style="color: #3465A4; "><b>credentials</b></span>
└── google-cloud-credentials.json
</pre>

For loading file in bucket env variable `GCS_BUCKET_ID` is required.
Bucket id can be found in config file `airflow/dags/configs/exchange_rates.yml`.

## Airflow Pipeline

Airflow Pipeline collects data for today. It uses custom Operator.

### Structure and Credentials
<pre>
<span style="color: #3465A4; "><b>airflow</b></span>
├── <span style="color: #3465A4; "><b>dags</b></span>
│   ├── <span style="color: #3465A4; "><b>credentials</b></span>
│   │   └── google-cloud-credentials.json
│   ├── <span style="color: #3465A4; "><b>configs</b></span>
│   │   └── exchange_rates.yml
│   └── exchange_rates_dag.py
└── <span style="color: #3465A4; "><b>include</b></span>
    └── exchange_api_operator.py

</pre>

Also, for loading files to GCP bucket is necessary to create connection `google_cloud_default` with parameters:
* <b>Connection Type</b>: `Google Cloud`  
* <b>Keyfile Path</b>: `/usr/local/airflow/dags/credentials/google-cloud-credentials.json`
