import logging
from datetime import datetime, timezone
from importlib import resources as impresources
from google.cloud import bigquery
from bigquery_etl_tools import (
    BigqueryEtlClient
)
from bigquery_etl_tools.bigquery_utils import table_exists

from ..config import get_config
from ..fpl import FplClient
from .. import templates


class FplBigqueryClient(FplClient):
    """
    class for uploading tables from fantasy premier league to bigquery
    """
    def __init__(self,
                 config: dict,
                 bucket: str,
                 dataset: str
                 ) -> None:
        super().__init__(config)
        self.BUCKET = bucket
        self.DATASET = dataset
        self.etl_client = BigqueryEtlClient(self.BUCKET)
        self.storage_client = self.etl_client.storage_client
        self.bigquery_client = self.etl_client.bigquery_client

    def load_bigquery_schema(
            self,
            table_name: str
            ) -> list:
        """
        load bigquery schema from location
        @return bigquery schema list
        """
        schema_path = f'bigquery/schemas/{table_name}.json'
        inp_file = impresources.files(templates) / schema_path
        logging.info("Loading schema from %s", inp_file)
        with inp_file.open('r') as file:
            schema = self.etl_client.bigquery_client.schema_from_json(file)
        return schema

    def __ingest_table_inner(
            self,
            endpoint_name: str,
            table_name: str,
            refresh: bool = False,
            endpoint_kwargs: dict = {},
            load_job_kwargs: dict = {}
            ) -> bigquery.Table:
        """
        ingest a table from the fantasy.premierleague endpoint into bigquery
        @param endpoint_name name of the endpoint to download
        @param table_name name of table
        @param refresh boolean to refresh endpoint data, cache used if false
            and cache exists
        @param endpoint_kwargs keyword arguments for the endpoint function
        @param load_job_kwargs keyword arguments for the bigquery load job to
            overwrite defaults
        @return None
        """

        logging.info("Downloading table %s from endpoint %s",
                     table_name, endpoint_name)
        df = self.get_table(
            endpoint_name, table_name, refresh, endpoint_kwargs)

        endpoint_config = self.config_api['endpoints'][endpoint_name]
        table_config = endpoint_config['tables'][table_name]
        file_type = table_config.get('file_type', endpoint_config['file_type'])

        default_load_job_kwargs = get_config(
            table_config.get('bigquery_config', {}),
            endpoint_config['bigquery_config']
        )
        for k, v in load_job_kwargs.items():
            default_load_job_kwargs[k] = v
        load_job_kwargs = default_load_job_kwargs

        job_config = bigquery.LoadJobConfig(
            **load_job_kwargs
        )

        use_schema = table_config.get('use_schema', False)
        if use_schema:
            job_config.schema = self.load_bigquery_schema(table_name)
        else:
            logging.info("No schema specified for %s in config, autodetecting",
                         table_name)
            job_config.autodetect = True

        now_ts = int(round(datetime.now(timezone.utc).timestamp()))
        blob_dir = f'this_season/{table_name}'
        blob_name = f'{blob_dir}/{now_ts}_{table_name}.{file_type}'
        table_id = f'{self.DATASET}.{table_name}'

        logging.info("Uploading table %s to bigquery", table_name)
        blob, table = self.etl_client.dataframe_to_bigquery(
            dataframe=df,
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

        return table

    def ingest_table(
            self,
            *args,
            **kwargs):
        """
        wrapper for the ingest_table function to handle errors and gather meta
        data
        @param *args arguments for the ingest_table function
        @param **kwargs keyword arguments for the ingest_table function
        @return meta data dictionary
        """
        try:
            # table_before = self.get_table_meta()
            _ = self.__ingest_table_inner(*args, **kwargs)
            # table_after = self.get_table_meta()
            meta = {
                'status': 'success'
                # 'before': table_before,
                # 'after': table_after
            }

        except Exception as e:
            logging.error("""The table %s from endpoint %s failed to ingest
                          with the following message: %s""", e)
            meta = {
                'status': 'fail',
                'error': e
            }
        return meta

    def ingest_bootstrap_static(
            self,
            table_subset: list = None,
            remove_cache: bool = True):
        """
        ingest all tables from the bootstrap static endpoint
        @param table_subset list of tables to download
        @param remove_cache remove endpoint from cache after ingestion
        @return meta data dictionary
        """
        meta = {}
        ENDPOINT = 'bootstrap_static'
        tables = self.list_endpoint_tables(ENDPOINT, ingest_only=True)
        if table_subset:
            tables = [table for table in tables if table in table_subset]
        for table in tables:
            meta[table] = self.ingest_table(
                ENDPOINT, table)
        if remove_cache:
            self.release_endpoint(ENDPOINT)
        return meta

    def ingest_element_summary(
            self,
            teams: list,
            remove_cache: bool = True,
            full_refresh: bool = False):
        """
        ingest all tables from the element summary endpoint, with a subset of
        teams
        @param teams list of team ids to download player from
        @param remove_cache remove endpoint from cache after ingestion
        @param full_refresh refresh endpoint data
        @return meta data dictionary
        """
        meta = {}
        ENDPOINT = "element_summary"
        tables = self.list_endpoint_tables(ENDPOINT, ingest_only=True)
        endpoint_kwargs = {'elements': self.list_elements(teams)}
        if full_refresh:
            load_job_kwargs = {'write_disposition': 'WRITE_TRUNCATE'}
        else:
            load_job_kwargs = {}
        for table in tables:
            meta[table] = self.ingest_table(
                ENDPOINT,
                table,
                endpoint_kwargs=endpoint_kwargs,
                load_job_kwargs=load_job_kwargs)
        if remove_cache:
            self.release_endpoint(ENDPOINT)
        return meta

    def load_player_history_from_uri(
            self,
            date_string: str
            ) -> bigquery.LoadJob:
        """
        Load the player history csv's from uri
        @param date_string indicates the date the player data was loaded
            this controls where the data will be stored in cloud storage
        @return job the bigquery load job
        """

        bigquery_client = bigquery.Client()

        job_config = bigquery.LoadJobConfig(
            source_format='CSV',
            write_disposition='WRITE_TRUNCATE',
            schema=self.load_bigquery_schema('history'),
            skip_leading_rows=1
        )

        blob_dir = f'this_season/history/full_refresh/{date_string}'
        job = bigquery_client.load_table_from_uri(
            f'gs://{self.BUCKET}/{blob_dir}/*',
            'fantasy_premier_league.history',
            job_config=job_config
        )
        job.result()

        return job
