"""module containing functions to run the fpl_connector pipeline"""
import logging
from datetime import datetime, timedelta

from fpl_connector import (
    FplBigqueryClient,
    load_config
)
from fpl_connector.utils.polars_utils import dataframe_to_list


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
        test: bool = False,
        output_dir: str = 'this_season'):
    """
    run function to move element summary player history to cloud storage
        for a set of teams
    @param teams a list of team ids
    @param test boolean indicating whether this is a test run
    @param output_dir the base blobdir in the bucket to place results
    @return None
    """
    ENDPOINT = "element_summary"
    today_dt = datetime.now()
    if test:
        teams = [1, 2]

    config = load_config()
    fpl_client = FplBigqueryClient(config)
    table = "history"
    logging.info("Ingest prepared for the following tables %s", table)
    for team in teams:
        logging.info("Downloading players from team %s", team)
        dataframe = fpl_client.get_table(
            ENDPOINT,
            table,
            endpoint_kwargs={'elements': fpl_client.list_elements(team)}
        )
        todays_file = f'{today_dt.strftime("%Y%m%d")}/{table}_{team}.csv'
        output_file = f'{output_dir}/{table}/full_refresh/{todays_file}'
        fpl_client.etl_client.dataframe_to_storage(
            dataframe,
            output_file,
            'csv'
        )
        logging.info(
            "Releasing endpoint from cache %s and downloading new team",
            ENDPOINT)
        fpl_client.release_endpoint(ENDPOINT)


def run_players_to_bigquery():
    pass
