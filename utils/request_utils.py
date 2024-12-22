import logging
import requests


def get_endpoint_wrapper(endpoint: str) -> dict:

    r = requests.get(endpoint)

    if r.status_code == 200:
        return r.json()

    else:
        logging.error(f'[{r.status_code}] API call failed')
        logging.error(r.content)
        return None
