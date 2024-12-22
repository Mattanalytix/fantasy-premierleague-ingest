import logging
from utils.request_utils import get_endpoint_wrapper
from config import load_config

config = load_config()
config_api = config['api']
API_BASE = config_api['base']


def get_bootstrap_static() -> dict:
    """
    download the data from the bootstrap-static endpoint
    @return json contents of the endpoint
    """
    endpoint = f'{API_BASE}/bootstrap-static'
    logging.info('Downloading endpoint %s', endpoint)
    return {'bootstrap_static': get_endpoint_wrapper(endpoint)}


def get_fixtures():
    """
    download the data from the fixtures endpoint
    @return json contents of the endpoint
    """
    endpoint = f'{API_BASE}/fixtures'
    logging.info('Downloading endpoint %s', endpoint)
    return {'fixtures': {'fixtures': get_endpoint_wrapper(endpoint)}}


def get_element_summary(elements: list, history_past: bool = False):
    """
    download the data from the element-summary endpoint
    @param elements list of element ids to download
    @param history_past 
    @return json contents of the endpoint
    """
    element_summary_dict = {
        'history': []
    }

    if history_past:
        element_summary_dict['history_past'] = []

    for element in elements:
        endpoint = f'{API_BASE}/element-summary/{element}'
        logging.info('Downloading endpoint %s', endpoint)
        json = get_endpoint_wrapper(endpoint)
        missing_keys = set(element_summary_dict.keys()) - set(json.keys())
        assert missing_keys == set(), \
            f"The following keys are missing {missing_keys}"

        if history_past:
            for row in json['history_past']:
                row.update({'element_id': element})

        for k in element_summary_dict.keys():
            element_summary_dict[k].extend(json[k])

    return {'element_summary': element_summary_dict}


# @TODO add pytest to check this works for all configured endpoints
DOWNLOAD_ENDPOINT_LOOKUP = {
    'bootstrap_static': get_bootstrap_static,
    'fixtures': get_fixtures,
    'element_summary': get_element_summary
}
