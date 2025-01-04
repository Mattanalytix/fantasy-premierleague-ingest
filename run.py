"""module containing functions to run the fpl_connector pipeline"""
import logging
from datetime import datetime, timedelta

from fpl_connector import (
    FplClient,
    FplBigqueryClient,
    load_config
)
from fpl_connector.utils.polars_utils import dataframe_to_list
from bigquery_etl_tools import BigqueryEtlClient


def run_daily_pipeline(test: bool = False):
    config = load_config()
    fpl_client = FplBigqueryClient(config)

    meta = {'tables': {}}

    logging.info("Uploading the latest fixtures to bigquery ...")
    fpl_client.ingest_table('fixtures', 'fixtures')

    logging.info("Getting yesterdays fixtures")
    yesterday_date = (datetime.now().date() - timedelta(days=1))
    yesterdays_fixtures, teams = fpl_client.get_fixtures(yesterday_date)
    meta['fixtures'] = dataframe_to_list(yesterdays_fixtures)

    bootstrap_tables = fpl_client.list_endpoint_tables(
        'bootstrap_static', ingest_only=True)
    if test:
        teams = 1
        bootstrap_tables = ['events', 'teams']

    if yesterdays_fixtures.shape[0] > 0 or test:
        endpoints = fpl_client.list_endpoints()
        logging.info("The following endpoints are configured %s", endpoints)
        endpoints.remove('fixtures')
        logging.info("Running ETL process for bootstrap static")
        meta['tables'].update(fpl_client.ingest_bootstrap_static(
            bootstrap_tables))
        logging.info("Running ETL process for element history")

        meta['tables'].update(fpl_client.ingest_element_summary(teams))
        logging.info("ETL Successfull, exiting application ...")
    else:
        logging.info("No fixtures played yesterday (%s)", yesterday_date)
        logging.info("Exiting application ...")

    return meta


def run_players_to_storage(
        teams: list,
        bucket_name: str,
        test: bool = False,
        output_dir: str = 'this_season') -> None:
    """
    run function to move element summary player history to cloud storage
        for a set of teams
    @param teams a list of team ids
    @param bucket_name the name of the bucket to upload to
    @param test boolean indicating whether this is a test run
    @param output_dir the base blobdir in the bucket to place results
    @return None
    """
    ENDPOINT = "element_summary"
    today_dt = datetime.now()
    if test:
        teams = [1, 2]

    config = load_config()
    fpl_client = FplClient(config)
    etl_client = BigqueryEtlClient(
        bucket_name=bucket_name
    )
    table = "history"
    logging.info("Ingest prepared for the following tables %s", table)
    for team in teams:
        logging.info("Downloading players from team %s", team)
        dataframe = fpl_client.get_table(
            ENDPOINT,
            table,
            endpoint_kwargs={'elements': fpl_client.list_elements([team])}
        )
        todays_file = f'{today_dt.strftime("%Y%m%d")}/{table}_{team}.csv'
        output_file = f'{output_dir}/{table}/full_refresh/{todays_file}'
        etl_client.dataframe_to_storage(
            dataframe,
            output_file,
            'csv'
        )
        logging.info(
            "Releasing endpoint from cache %s and downloading new team",
            ENDPOINT)
        fpl_client.release_endpoint(ENDPOINT)


def run_players_to_bigquery(
        date_string: str,
        bucket_name: str,
        dataset_name: str
        ) -> None:
    """
    run function to move element summary player history to bigquery
        from google cloud storage using a wildcard
    @param date_string the date the data was loaded to cloud storage
        must match the format of the blobdir
    @param bucket_name the name of the bucket to upload to
    @param dataset_name the name of the dataset to upload to
    @return None
    """
    config = load_config()
    fpl_client = FplBigqueryClient(
        config,
        bucket_name,
        dataset_name
    )

    _ = fpl_client.load_player_history_from_uri(
        date_string
    )
