import logging
import yaml


def load_config() -> dict:
    """
    load application config from config.yml
    @return application config dict
    """
    with open('config.yml', 'r') as file:
        return yaml.safe_load(file)


def validate_config_endpoint(config_endpoint: dict) -> None:
    """
    validate the configuration for an endpoint looks as expected
    @param config_endpoint the endpoint configuration dict
    """
    config_endpoint_error_keys = {'name', 'config', 'tables'} - set(
        config_endpoint.keys())
    assert len(config_endpoint_error_keys) == 0, f"""
        Endpoint configuration for {config_endpoint['name']} missing required
        values {config_endpoint_error_keys}"""

    try:
        config_endpoint_warning_keys = {'description'} - set(
            config_endpoint.keys())
        assert len(config_endpoint_warning_keys) == 0, f"""Endpoint
            configuration for {config_endpoint['name']} missing desired values
            {config_endpoint_warning_keys}"""
    except AssertionError as e:
        logging.warning(e)


def get_table_config(table: dict, default_config: dict) -> dict:
    """
    get table config and overwrite defaults if config set for table
    @param table config for the table
    @param default_config default config for all tables
    @return table config with defaults
    """
    table_config = table.get('config', {})
    for k, v in default_config.items():
        if k not in table_config:
            table_config[k] = v
        else:
            logging.debug("""Overwritting default config for '%s' to %s for
                          table %s""", k, v, table['name'])
    return table_config
