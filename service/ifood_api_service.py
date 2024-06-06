import logging
import requests
import json
from util.properties_reader import property_value

logger = logging.getLogger(__name__)

ifood_url = property_value('ApiUrl', 'ifoodMercado')


def get_orders_details(id_number, token):
    try:
        if token is None:
            return None

        url = ifood_url + f"pedido/{id_number}"

        payload = {}
        headers = {'accept': 'application/json', 'Authorization': f'Bearer {token["access_token"]}'}

        response = requests.request("GET", url, headers=headers, data=payload, verify=False)

        if response.status_code > 300:
            logger.error("ERROR: Get Ifood order details.\n" + str(response))
            return None

        response = json.loads(response.text)
    except Exception as error:
        logger.error(f"ERROR: Get Ifood order details.\n{error}")
        return None

    return response
