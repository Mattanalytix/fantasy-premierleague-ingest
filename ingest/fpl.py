import os
import logging
import requests
from datetime import datetime, timezone

import polars as pl

from config import get_table_config
from bigquery_etl_tools import (
    dataframe_to_bigquery
)
from bigquery_etl_tools.bigquery_utils import table_exists


class FplIngest:
    """
    class for ingesting data from the fantasy.premierleague api
    """
    def __init__(self,
                 config):
        self.config_api = config['api']
        self.__env = {}
        for name, value in config['env'].items():
            self.__env[name] = os.environ[value]
        self.endpoints = {}
        self.bucket = self.__env["bucket"]
        self.blobdir = self.__env["blobdir"]
        self.dataset = self.__env["dataset"]

    def list_endpoints(self) -> list:
        """
        list the available configured endpoints
        @return list of available endpoints
        """
        return list(self.config_api['endpoints'].keys())

    def get_endpoint(self, endpoint_name: str) -> dict:
        """
        download json contents of an endpoint to a python dictionary
        @param endpoint_name name of the endpoint to download
        @return json contents of the endpoint
        """
        config_endpoint = self.config_api['endpoints'][endpoint_name]
        api_base = self.config_api['base']
        endpoint = f'{api_base}/{config_endpoint["name"]}'
        logging.info('Downloading endpoint %s', endpoint)
        r = requests.get(endpoint)

        if r.status_code == 200:
            self.endpoints[endpoint_name] = r.json()
            return r.json()

        else:
            logging.error(f'[{r.status_code}] API call failed')
            logging.error(r.content)
            logging.info('Try again later ...')
            return None

    def release_endpoint(self, endpoint_name: str) -> None:
        """
        release endpoint data from cache
        @param endpoint_name name of the endpoint to download
        @return None
        """
        logging.info('Releasing endpoint %s', endpoint_name)
        self.endpoints.pop(endpoint_name)

    def get_table(
            self,
            endpoint_name: str,
            table_name: str,
            refresh: bool = False) -> pl.DataFrame:
        """
        get table from endpoint
        @param endpoint_name name of the endpoint to download
        @param table_name name of table
        @param refresh boolean to refresh endpoint data, cache used if false
            and cache exists
        @return table as a polars dataframe
        """
        if refresh or endpoint_name not in self.endpoints:
            logging.info('Refreshing endpoint %s', endpoint_name)
            endpoint_dict = self.get_endpoint(endpoint_name)
        else:
            logging.info('Using cached endpoint %s', endpoint_name)
            endpoint_dict = self.endpoints[endpoint_name]
        table_dict = endpoint_dict[table_name]
        return pl.DataFrame(table_dict)

    def ingest_table(
            self,
            endpoint_name: str,
            table_name: str,
            refresh: bool = False) -> None:

        df = self.get_table(endpoint_name, table_name, refresh)

        config_endpoint = self.config_api['endpoints'][endpoint_name]

        table_config = get_table_config(
            config_endpoint['tables'][table_name],
            config_endpoint['default_table_config']
        )
        file_type = table_config['file_type']
        now_ts = int(round(datetime.now(timezone.utc).timestamp()))
        blob_name = f'{self.blobdir}/{now_ts}_{table_name}.{file_type}'
        table_id = f'{self.dataset}.{table_name}'

        blob, table = dataframe_to_bigquery(
            dataframe=df,
            bucket_name=self.bucket,
            blob_name=blob_name,
            table_id=table_id,
            file_type=file_type
        )

        assert blob.exists(), f"""Blob {blob.name} does not exist"""
        assert table_exists(table), f"""Table {table.table_id} does
            not exist"""
        assert datetime.timestamp(table.modified) - now_ts > 0, """
            Table not updated"""
