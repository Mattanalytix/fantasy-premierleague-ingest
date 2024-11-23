import os
import logging
from datetime import datetime, timezone
import polars as pl
from google.cloud import storage, bigquery
from dotenv import load_dotenv
import requests
from bigquery_etl_tools import (
    dataframe_to_bigquery
)
from bigquery_etl_tools.bigquery_utils import table_exists

from config import load_config, validate_config_endpoint, get_table_config


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)
load_dotenv()

BUCKET_NAME = os.environ['BUCKET']
DATASET_NAME = os.environ['DATASET']
BLOB_DIR = 'test'

bigquery_client = bigquery.Client()
storage_client = storage.Client()

bucket = storage_client.get_bucket(BUCKET_NAME)


if __name__ == "__main__":

    config = load_config()
    config_api = config['api']
    logging.info("""Loaded config for api: %s""", config_api['base'])
    logging.info("""Iterating over the following endpoints
                 %s ...""", list(config_api['endpoints'].keys()))
    base = config['api']['base']
    for config_endpoint in config_api['endpoints'].values():
        endpoint = f'{base}/{config_endpoint["name"]}'
        r = requests.get(endpoint)

        # @TODO extract this as a pytest, no need to run every time
        logging.debug('Validating the endpoint configuration for %s ...',
                      config_endpoint['name'])
        validate_config_endpoint(config_endpoint)
        logging.debug('Configuration validated for %s ...',
                      config_endpoint['name'])

        logging.info(
            """Loaded config for %s/%s: %s ...""",
            base, config_endpoint['name'], config_endpoint['description'])

        # @TODO write verification function for default table settings (also
        # should be pytest)
        default_config = config_endpoint['config']
        table_names = [x['name'] for x in config_endpoint['tables']]

        logging.info("""
                    Iterating over the following tables %s ...""", table_names)
        for table in config_endpoint['tables']:
            table_config = get_table_config(table, default_config)
            if table_config['ingest']:
                df = pl.DataFrame(r.json())
                now_ts = int(round(datetime.now(timezone.utc).timestamp()))
                table_name = table["name"]
                file_type = table_config['file_type']
                # @TODO rename blob
                blob_name = f'{BLOB_DIR}/{now_ts}_{table_name}.{file_type}'
                table_id = f'{DATASET_NAME}.{table_name}'

                blob, table = dataframe_to_bigquery(
                    dataframe=df,
                    bucket_name=BUCKET_NAME,
                    blob_name=blob_name,
                    table_id=table_id,
                    file_type=file_type
                )

                assert blob.exists(), f"""Blob {blob.name} does not exist"""
                assert table_exists(table), f"""Table {table.table_id} does
                    not exist"""
                assert datetime.timestamp(table.modified) - now_ts > 0, """
                    Table not updated"""
