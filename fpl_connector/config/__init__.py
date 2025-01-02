import logging
import yaml


def load_config() -> dict:
    """
    load application config from config.yml
    @return application config dict
    """
    with open('config/config.yml', 'r') as file:
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


def get_config(config: dict, default: dict) -> dict:
    """
    get config and use default args if none set
    @param table config for the table
    @param default_config default config for all tables
    @return table config with defaults
    """
    for k, v in default.items():
        if k not in config:
            config[k] = v
        else:
            logging.debug("""Overwritting default config for '%s' to %s for
                          """, k, v)
    return config
