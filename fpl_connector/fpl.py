import logging
from datetime import datetime
import polars as pl

from .download import (
    get_bootstrap_static,
    get_fixtures,
    get_element_summary
)
from .transform import (
    transform_fixtures
)


# @TODO add pytest to check this works for all configured endpoints
DOWNLOAD_LOOKUP = {
    'bootstrap_static': get_bootstrap_static,
    'fixtures': get_fixtures,
    'element_summary': get_element_summary
}

TRANSFORM_LOOKUP = {
    'fixtures': transform_fixtures
}


class FplClient:
    """
    class for interacting with the fantasy.premierleague api
    """
    def __init__(self,
                 config: dict) -> None:
        self.config_api = config['api']
        self.endpoints = {}

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
            endpoint_kwargs: dict = {}
            ) -> pl.DataFrame:
        """
        get table from endpoint
        @param endpoint_name name of the endpoint to download
        @param table_name name of table
        @param refresh boolean to refresh endpoint data, cache used if false
            and cache exists
        @param endpoint_kwargs keyword arguments for the endpoint function
        @return table as a polars dataframe
        """
        if refresh or endpoint_name not in self.endpoints:
            logging.info('Refreshing endpoint %s', endpoint_name)
            endpoint_dict = self.get_endpoint(endpoint_name, **endpoint_kwargs)
        else:
            logging.info('Using cached endpoint %s', endpoint_name)
            endpoint_dict = self.endpoints[endpoint_name]

        return TRANSFORM_LOOKUP.get(table_name, lambda x: x)(
            pl.DataFrame(endpoint_dict[table_name]))

    def get_fixtures(
            self,
            fixture_date: datetime.date
            ) -> tuple[pl.DataFrame, list]:
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

        teams = pl.Series((
            fixtures_filtered
            .unpivot(['team_h', 'team_a'])
            .select(['value'])
            .unique()
        )).to_list()

        return fixtures_filtered, teams

    # @TODO add unit test to make sure default gets all elements
    def list_elements(
            self,
            teams=range(1, 21)
            ) -> list:
        """
        get a list of element ids (players) from a list of team ids
        @param teams list of team ids
        @return list of element ids
        """
        start, end = 1, 20
        assert not any((team < start or team > end) for team in teams), f"""\
        The following teams are out of index: \
        {[team for team in teams if (team < start or team > end)]}"""

        endpoints = self.list_endpoints()
        players = self.get_table(endpoints[0], 'elements')
        return pl.Series((
            players
            .filter(pl.col('team').is_in(teams))
            .select(['id'])
        )).to_list()

    def list_endpoint_tables(
            self,
            endpoint_name: str,
            ingest_only: bool = False
            ) -> list:
        """
        list the configured tables available from an endpoint
        @param endpoint_name name of the endpoint to download
        @param ingest_only only list tables that are configured to ingest
        @return list of available tables
        """
        table_list = []
        endpoint_config = self.config_api['endpoints'][endpoint_name]
        for k, v in endpoint_config['tables'].items():
            if not ingest_only or ('ingest' not in v):
                table_list.append(k)

            elif v['ingest']:
                table_list.append(k)

            else:
                continue

        logging.info("""The endpoint %s has the following tables configured to
                     ingest %s""", endpoint_name, table_list)

        return table_list
