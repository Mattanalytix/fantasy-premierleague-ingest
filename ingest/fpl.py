import os
import logging
from datetime import datetime, timezone

import polars as pl

from config import get_table_config
from ingest.download import (
    get_bootstrap_static,
    get_fixtures,
    get_element_summary
)
from ingest.transform import (
    transform_fixtures
)
from bigquery_etl_tools import (
    dataframe_to_bigquery
)
from bigquery_etl_tools.bigquery_utils import table_exists


# @TODO add pytest to check this works for all configured endpoints
DOWNLOAD_LOOKUP = {
    'bootstrap_static': get_bootstrap_static,
    'fixtures': get_fixtures,
    'element_summary': get_element_summary
}

TRANSFORM_LOOKUP = {
    'fixtures': transform_fixtures
}


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

    def get_endpoint(self, endpoint_name: str, *args, **kwargs) -> dict:
        """
        download json contents of an endpoint to a python dictionary
        @param endpoint_name name of the endpoint to download
        @param *args arguments for the endpoint function
        @param **kwargs keyword arguments for the endpoint function
        @return json contents of the endpoint
        """
        self.endpoints.update(DOWNLOAD_LOOKUP[
            endpoint_name
        ](*args, **kwargs))
        return self.endpoints[endpoint_name]

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
            refresh: bool = False,
            endoint_kwargs: dict = {}
            ) -> pl.DataFrame:
        """
        get table from endpoint
        @param endpoint_name name of the endpoint to download
        @param table_name name of table
        @param refresh boolean to refresh endpoint data, cache used if false
            and cache exists
        @param endoint_kwargs keyword arguments for the endpoint function
        @return table as a polars dataframe
        """
        if refresh or endpoint_name not in self.endpoints:
            logging.info('Refreshing endpoint %s', endpoint_name)
            endpoint_dict = self.get_endpoint(endpoint_name, **endoint_kwargs)
        else:
            logging.info('Using cached endpoint %s', endpoint_name)
            endpoint_dict = self.endpoints[endpoint_name]

        return TRANSFORM_LOOKUP.get(table_name, lambda x: x)(
            pl.DataFrame(endpoint_dict[table_name]))

    def get_fixtures(
            self,
            fixture_date: datetime.date
            ) -> pl.DataFrame:
        """
        get fixture list on a given date
        @param fixture_date date to get fixtures for
        @return table as a polars dataframe
        """
        logging.info("Getting fixtures from %s", fixture_date)
        endpoints = self.list_endpoints()

        fixtures = self.get_table(endpoints[1], 'fixtures')
        teams = self.get_table(endpoints[0], 'teams')

        fixtures_filtered = (
            fixtures
            .select(['kickoff_time', 'team_h', 'team_a'])
            .filter(pl.col("kickoff_time").dt.date() == fixture_date)
            .join(
                teams.select(['id', 'name']),
                left_on="team_h",
                right_on="id",
                how='left')
            .rename({'name': 'team_h_name'})
            .join(
                teams.select(['id', 'name']),
                left_on="team_a",
                right_on="id",
                how='left')
            .rename({'name': 'team_a_name'})
        )

        return fixtures_filtered

    # @TODO add unit test to make sure default gets all elements
    def get_element_list(
            self,
            teams=range(1, 21)
            ) -> list:
        """
        get a list of element ids (players) from a list of team ids
        @param teams list of team ids
        @return list of element ids
        """
        endpoints = self.list_endpoints()
        players = self.get_table(endpoints[0], 'elements')
        return pl.Series((
            players
            .filter(pl.col('team').is_in(teams))
            .select(['id'])
        )).to_list()

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
