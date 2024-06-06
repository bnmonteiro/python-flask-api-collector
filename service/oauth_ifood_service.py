import logging
import requests
import json
import os
from datetime import datetime, timedelta
from service.token_singleton import TokenSingleton
from util.properties_reader import property_value

logger = logging.getLogger(__name__)


class OauthIFoodClient:

    def __init__(self):
        self.ifood_url = property_value('ApiUrl', 'ifoodMercado')
        self.headers = {'Content-Type': 'application/json'}

    @staticmethod
    def get_api_client_info():
        client_id = os.environ.get('CLIENT_ID_IFOOD_MERCADO')
        client_secret = os.environ.get('CLIENT_SECRET_IFOOD_MERCADO')

        if client_id is None or client_secret is None:
            return None

        return {"client_id": client_id, "client_secret": client_secret}

    @staticmethod
    def is_token_expired():
        token = TokenSingleton()

        if not hasattr(token, 'response'):
            return True

        if token.response['expiresInDate'] <= datetime.now():
            return True

        return False

    def get_token(self):
        try:

            if self.is_token_expired():
                url = self.ifood_url + "oauth/token"

                client_info = self.get_api_client_info()
                if client_info is None:
                    logger.error("ERROR: Unable to obtain Ifood Mercado API client info from OS variables")
                    return None

                payload = json.dumps({
                    "client_id": client_info["client_id"],
                    "client_secret": client_info["client_secret"]
                })

                response = requests.request("POST", url, headers=self.headers, data=payload, verify=False)

                if response.status_code != 200:
                    logger.error("ERROR: Unable to get token from Ifood API\n" + str(response))
                    return None

                token = TokenSingleton()
                token.response = json.loads(response.text)
                token.response['expiresInDate'] = datetime.now() + timedelta(seconds=int(token.response['expires_in']) - 120)
                # Token expires in 53 minutes

                return token.response

            return TokenSingleton().response

        except Exception as error:
            logger.error(f"ERROR: Unable to get token from Ifood Mercado API\n{error}")
            return None
