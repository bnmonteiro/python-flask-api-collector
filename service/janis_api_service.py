import logging
from datetime import time

import requests as requests
from requests.exceptions import ChunkedEncodingError

from util.properties_reader import property_value

logger = logging.getLogger(__name__)

api_url = property_value('ApiUrl', 'janis')

headers = {
    'janis-client': '**',
    'janis-api-key': '**',
    'janis-api-secret': '**'
}


def find_order(erp_number):
    url = f'{api_url}get?seqId={erp_number}'

    logger.info(f'Find janis order: {url}')

    try:
        response = requests.get(url, headers=headers)

        if not response:
            logger.info("janis Empty response received")
            return None

        data = response.json()

        if response.status_code == 200:
            return data
        if response.status_code == 404:
            logger.info("janis order not found")
            return None
        if response.status_code == 429:
            logger.info(f'Too many request. Sleep and retry.')
            time.sleep(1)
            return find_order(erp_number)
        if response.status_code == 509:## bad request. Retry

            logger.info(f'Bad Request. just retry.')
            return find_order(erp_number)
        else:
            logger.error(f'Find janis order Failed:{response.status_code} - {response} - {data}')
            return None
    except ChunkedEncodingError as e:
        logger.error(f'Fail janis ChunkedEncodingError in api. Retry...')
        return find_order(erp_number)
    except Exception as e:
        logger.error(f'Fail find janis order in api', exc_info=True)
        return None

