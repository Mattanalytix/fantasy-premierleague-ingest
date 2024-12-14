import logging
import warnings

from config import load_config


config = load_config()
config_api = config['api']


def test_endoint_configuration_errors():
    """
    validate the configuration for an endpoint looks as expected (it must
    contain the required keys name, config and tables)
    """
    for k, config_endpoint in config_api['endpoints'].items():
        logging.debug('Validating the endpoint configuration for %s ...',
                      k)
        config_endpoint_error_keys = {'name', 'config', 'tables'} - set(
            config_endpoint.keys())
        assert len(config_endpoint_error_keys) == 0, f"""
            Endpoint configuration for {k} missing
            required values {config_endpoint_error_keys}"""
        logging.debug('Configuration validated for %s ...',
                      k)


def test_endoint_configuration_warnings():
    """
    validate the configuration for an endpoint looks as expected (it should
    contain the required keys name, config and tables)
    """
    for k, config_endpoint in config_api['endpoints'].items():
        logging.debug('Validating the endpoint configuration for %s ...',
                      k)
        config_endpoint_warning_keys = {'description'} - set(
            config_endpoint.keys())
        try:
            assert len(config_endpoint_warning_keys) == 0, f"""Endpoint
                configuration for {k} missing desired
                values {config_endpoint_warning_keys}"""
            logging.debug('Configuration validated for %s ...',
                          k)
        except AssertionError as e:
            logging.warning(e)
            warnings.warn(UserWarning(e))
