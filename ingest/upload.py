import os
import logging
from datetime import datetime, timezone
from google.cloud import bigquery
from bigquery_etl_tools import (
    dataframe_to_bigquery
)
from bigquery_etl_tools.bigquery_utils import table_exists

from config import get_config
from ingest.fpl import FplClient


class FplUploader(FplClient):
    """
    class for uploading tables from fantasy premier league to bigquery
    """
    def __init__(self,
                 config: dict) -> None:
        super().__init__(config)
        self.__env = {}
        for name, value in config['env'].items():
            self.__env[name] = os.environ[value]
        self.BUCKET = self.__env["BUCKET"]
        self.BLOBDIR = self.__env["BLOBDIR"]
        self.DATASET = self.__env["DATASET"]

    def ingest_table(
            self,
            endpoint_name: str,
            table_name: str,
            refresh: bool = False,
            endoint_kwargs: dict = {}) -> None:
        """
        ingest a table from the fantasy.premierleague endpoint into bigquery
        @param endpoint_name name of the endpoint to download
        @param table_name name of table
        @param refresh boolean to refresh endpoint data, cache used if false
            and cache exists
        @param endoint_kwargs keyword arguments for the endpoint function
        @return None
        """

        logging.info("Downloading table %s from endpoint %s",
                     table_name, endpoint_name)
        df = self.get_table(
            endpoint_name, table_name, refresh, endoint_kwargs)

        endpoint_config = self.config_api['endpoints'][endpoint_name]
        table_config = endpoint_config['tables'][table_name]
        file_type = table_config.get('file_type', endpoint_config['file_type'])

        load_job_kwargs = get_config(
            table_config.get('bigquery_config', {}),
            endpoint_config['bigquery_config']
        )
        job_config = bigquery.LoadJobConfig(
            **load_job_kwargs
        )

        now_ts = int(round(datetime.now(timezone.utc).timestamp()))
        blob_name = f'{self.BLOBDIR}/{now_ts}_{table_name}.{file_type}'
        table_id = f'{self.DATASET}.{table_name}'

        logging.info("Uploading table %s to bigquery", table_name)
        blob, table = dataframe_to_bigquery(
            dataframe=df,
            bucket_name=self.BUCKET,
            blob_name=blob_name,
            table_id=table_id,
            file_type=file_type,
            job_config=job_config
        )

        # @TODO add function get_bigquery_table_meta() to bigquery_etl_tools

        assert blob.exists(), f"""Blob {blob.name} does not exist"""
        assert table_exists(table), f"""Table {table.table_id} does
            not exist"""
        assert datetime.timestamp(table.modified) - now_ts > 0, """
            Table not updated"""

    def ingest_endpoint():
        pass
