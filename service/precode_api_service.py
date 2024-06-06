import logging
import time
import requests as requests
import concurrent.futures

from util.properties_reader import property_value

logger = logging.getLogger(__name__)

api_url = property_value('ApiUrl', 'precodeOrderDate')

# Definindo o mapa com os tokens e nomes associados
tokens_map = {
    'TGN1MFd1R3M4Z2hQU3FRM2w6': 'CarrefourFood',
    'UnhBelNMQVF2SjVNR0xBck86': 'CarrefourFarma',
    'SkdvbzkwdjRSTXFvRkhqRm06': 'BigByCarrefour',
    'dWZzcm9DMzBXajJmMnVhQVo6': 'Nacional1',
    'UGlISW10NDR5QzRjQXBzZU46': 'Nacional2',
    'NjBtaGZWUGRLczM4ODNiQVU6': 'BomPreco1',
    'SHBZaDl0bHVyVUxFdjF1eVg6': 'BomPreco2',
    'YWV6amhJZmZjbVdSTmhyYWQ6': 'TodoDia1',
    'T1NmRVBPN09KaHFjaTRDaFQ6': 'TodoDia2'
}


def get_token_name(token):
    return tokens_map.get(token, None)


tokens_precode = ["TGN1MFd1R3M4Z2hQU3FRM2w6", "UnhBelNMQVF2SjVNR0xBck86", "SkdvbzkwdjRSTXFvRkhqRm06",
                  "dWZzcm9DMzBXajJmMnVhQVo6", "UGlISW10NDR5QzRjQXBzZU46", "NjBtaGZWUGRLczM4ODNiQVU6",
                  "SHBZaDl0bHVyVUxFdjF1eVg6", "YWV6amhJZmZjbVdSTmhyYWQ6", "T1NmRVBPN09KaHFjaTRDaFQ6"]

order_log_url = property_value('ApiUrl', 'precodeLog')
log_headers = {'Authorization': 'Basic Z2VmTGRPSllHZnJlNmMySFU6'}


def find_order_log(precode_id):
    url = f'{order_log_url}/{precode_id}'
    try:
        response = requests.get(url, headers=log_headers)
        data = response.json()
        if response.status_code == 200:
            logger.debug(f'find_order_log OK')
            return data["Description"]
        if response.status_code == 429:
            logger.error(f'find_order_log Too many request. Sleep and retry.')
            time.sleep(2)
            return find_order_log(precode_id)
        else:
            logger.error(f'Find order log Failed:{response}')
            return []
    except Exception as e:
        logger.error('Fail find order log in preorder api: ' + repr(e))
        return []


def get_all_orders_precode(first_date_time, last_date_time):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(find_orders, [first_date_time]*len(tokens_precode),
                                    [last_date_time]*len(tokens_precode), tokens_precode))

    return results


def find_orders(first_date_time, last_date_time, token):
    bandeira = get_token_name(token)

    first_date = first_date_time.strftime("%Y-%m-%dT%H:%M:00")
    last_date = last_date_time.strftime("%Y-%m-%dT%H:%M:00")

    url = f'{api_url}/firstDate/{first_date}/lastDate/{last_date}/0'
    headers = {'Authorization': f'Basic {token}'}

    logger.info(f'Find precode orders: {url} - Token: {token}')

    try:
        response = requests.get(url, headers=headers)
        data = response.json()

        if response.status_code == 200:
            return {"bandeira": bandeira, "data": data["list"]}
        if response.status_code == 429:
            logger.info(f'Too many request. Sleep and retry.')
            time.sleep(2)
            return find_orders(first_date_time, last_date_time, token)
        else:
            logger.error(f'Find order Failed:{response}:{data}')
            return []
    except Exception as e:
        logger.error('Fail load orders in preorder api: ' + repr(e))
        return []




