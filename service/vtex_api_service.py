import logging
from datetime import time
import requests as requests

from util.properties_reader import property_value

logger = logging.getLogger(__name__)

api_url = property_value('ApiUrl', 'vtex')

headers = {
    'X-VTEX-API-AppKey': '***',
    'X-VTEX-API-AppToken': '****'
}


def find_order(erp_number):
    url = f'{api_url}seq{erp_number}'

    logger.info(f'Find vtex order: {url}')

    try:
        response = requests.get(url, headers=headers)
        data = response.json()
        if response.status_code == 200:
            return data
        if response.status_code == 404:
            logger.info("Vtex order not found")
            return None
        if response.status_code == 429:
            logger.info(f'Too many request. Sleep and retry.')
            time.sleep(2)
            return find_order(erp_number)
        else:
            logger.error(f'Find vtex order Failed:{response}:{data}')
            return None
    except Exception as e:
        logger.error('Fail find vtex orders in api: ', exc_info=True)
        return None
