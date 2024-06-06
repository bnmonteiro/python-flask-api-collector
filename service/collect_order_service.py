import concurrent
import logging

from datetime import datetime, timedelta
from service.oauth_ifood_service import OauthIFoodClient
from service import precode_api_service as precode_service
from service import vtex_api_service as vtex_service
from service import janis_api_service as janis_service
from service import ifood_api_service as ifood_service
from util import utils
from dao import store_repository, order_repository, error_repository, vtex_repository, janis_repository

logger = logging.getLogger(__name__)

oauthIFoodClient = OauthIFoodClient()

checkout_error_codes = utils.load_error_codes()

CANCELLED_ERP_STATUS = 'Pedido cancelado pelo ERP'
CANCELLED_MKT_STATUS = 'Pedido Cancelado pelo Marketplace'

MONTH = 8
DAY = 1

HOUR_INI = 00
MINUTE_INI = 0
SECOND_INI = 0

HOUR_FIM = 23
MINUTE_FIM = 59
SECOND_FIM = 59


def add_store_data(order):
    branch = order["Branch"]
    store_code = branch.replace('carrefourbrfood', '')
    store_db = store_repository.find_by_code(store_code)

    if store_db:
        order["storeId"] = store_db["id"]
    else:
        logger.info('loja nao localizada')
        order["storeId"] = None


def add_system_error_data(order):
    if CANCELLED_MKT_STATUS in order["Status"]:
        order["SystemError"] = "IFOOD"
        return

    if CANCELLED_ERP_STATUS in order["Status"]:
        if int(order["OrderERP"]) > 10:
            order["SystemError"] = "JANIS"
        else:
            order["SystemError"] = "VTEX"
        return


def process_erp_error(order, order_log):
    located_errors = []
    error_found = False
    try:
        for item_log in order_log:
            log_text = item_log["Text"]
            if "Api Order Code Error" in log_text:
                code_error = retrieve_code_error(log_text)
                json_error = {
                    "OrderPrecode": order["OrderPrecode"],
                    "CodeError": code_error,
                    "Message": log_text,
                    "Date": item_log["Date"],
                    "SystemError": order["SystemError"],
                    "Meaning": find_error_meaning(code_error, log_text),
                    "Brand": retrieve_brand_error(order)}
                located_errors.append(json_error)
                error_found = True
                break
    except Exception:
        pass

    if not error_found:  ##find api simulation
        try:
            for item_log in order_log:
                log_text = item_log["Text"]
                if "Api Simulation Code Error" in log_text:
                    code_error = retrieve_code_error(log_text)
                    json_error = {
                        "OrderPrecode": order["OrderPrecode"],
                        "CodeError": code_error,
                        "Message": log_text,
                        "Date": item_log["Date"],
                        "SystemError": order["SystemError"],
                        "Meaning": find_error_meaning(code_error, log_text),
                        "Brand": retrieve_brand_error(order)}
                    located_errors.append(json_error)
                    error_found = True
                    break
        except Exception:
            pass

    if not error_found:  # find last Api Order Message
        messages = []
        try:
            for item_log in order_log:
                if "Api Order Message" in item_log["Text"]:
                    messages.append(item_log)
                    last_api_message = max(messages, key=lambda x: x["Date"])
                    text = last_api_message["Text"]
                    meaning = text.split("Message: ")[1]
                    json_error = {
                        "OrderPrecode": order["OrderPrecode"],
                        "CodeError": meaning.upper().strip().replace(" ", ""),
                        "Message": text,
                        "Date": item_log["Date"],
                        "SystemError": order["SystemError"],
                        "Meaning": meaning,
                        "Brand": retrieve_brand_error(order)
                    }
                    located_errors.append(json_error)
                    error_found = True
                    break
        except Exception:
            pass

    if not error_found:  # find message erro create on vtex
        try:
            for item_log in order_log:
                if "atingiu o limite de tentativas de cadastro na vtex." in item_log["Text"]:
                    json_error = {
                        "OrderPrecode": order["OrderPrecode"],
                        "CodeError": "FAILCREATEVTEX",
                        "Message": item_log["Text"],
                        "Date": item_log["Date"],
                        "SystemError": order["SystemError"],
                        "Meaning": item_log["Text"],
                        "Brand": retrieve_brand_error(order)
                    }
                    located_errors.append(json_error)
                    error_found = True
                    break
        except Exception:
            pass

    if not error_found:  # create msg for error not found
        logger.info('create msg for not found')

        json_error = {
            "OrderPrecode": order["OrderPrecode"],
            "CodeError": "Desconhecido",
            "Message": "Desconhecido",
            "Date": order["UpdateData"],
            "SystemError": order["SystemError"],
            "Meaning": "Desconhecido",
            "Brand": retrieve_brand_error(order)
        }

        if order["OrderIdIfood"] is not None:
            order_error_details = error_repository.find_by_number_ifood(order["OrderIdIfood"])
            if order_error_details:
                try:
                    json_error = {
                        "OrderPrecode": order["OrderPrecode"],
                        "CodeError": order_error_details["cancel_code"],
                        "Message": order_error_details["cancel_reason"],
                        "Date": order["UpdateData"],
                        "SystemError": order["SystemError"],
                        "Meaning": order_error_details["cancel_code_description"],
                        "Brand": retrieve_brand_error(order)
                    }
                except Exception as error:
                    logger.error(f"Erro na captura dos dados de cancelamento de chamado da tabela Ifood error: {error}")
                    pass

    error_repository.insert(json_error)


def retrieve_brand_error(order):
    if "Brand" in order:
        return order["Brand"]
    else:
        return "Loja Não Encontrada"


def find_error_meaning(code_error, log_text):
    try:
        meaning = checkout_error_codes[code_error]
        meaning = utils.translate_to_portuguese(meaning)
    except Exception:
        meaning = log_text.split("Message: ")[1]
    return meaning


def retrieve_code_error(log_text):
    separated_items = log_text.split(": ")
    try:
        item2_array = separated_items[2].split()
        code_error = item2_array[0]
    except Exception:
        try:
            code_error = log_text.split("Code Error")[1].split("Message")[0].strip()
        except Exception as error:
            logger.warning(f"{error}")
            code_error = "Erro"
    return code_error


def process_marketplace_error(order, order_log):
    # Check for EAN Unavailability
    for item_log in order_log:
        if "Pedido cancelado porque o EAN: " in item_log["Text"] \
                and "não está disponivel no catalogo do iFood" in item_log["Text"]:

            json_error = {
                "OrderPrecode": order["OrderPrecode"],
                "CodeError": "EAN Indisponível",
                "Message": item_log["Text"],
                "Date": item_log["Date"],
                "SystemError": "IFOOD",
                "Meaning": "Pedido alterado, incluído novo Produto que não possui EAN cadastrado no Ifood"
            }
            error_repository.insert(json_error)
            return

    # Check for Janis Status
    if order["janisId"]:
        janis_db = janis_repository.find_by_id(order["janisId"])
        if janis_db:
            current_status = janis_db["status_description"]
            previous_status = janis_db["previous_status"]
            logger.info(f'Cancelado MKT Place status anterior: [{previous_status}] - status atual: [{current_status}]')

            if current_status == "Auditoria pendente" or current_status == "Coleta pendente" or previous_status == "Auditoria pendente" or previous_status == "Coleta pendente":
                code = "OPERATIONAL_FAIL"
                message = "Falha Operacional"
                json_error = {
                    "OrderPrecode": order["OrderPrecode"],
                    "CodeError": code,
                    "Message": message,
                    "Date": order["UpdateData"],
                    "SystemError": "IFOOD",
                    "Meaning": message
                }
                error_repository.insert(json_error)
                return

    # Default Case
    msg_unknown = 'Cancelamento Marketplace - Motivo Desconhecido'
    json_error = {
        "OrderPrecode": order["OrderPrecode"],
        "CodeError": "Desconhecido",
        "Message": msg_unknown,
        "Date": order["UpdateData"],
        "SystemError": "IFOOD",
        "Meaning": msg_unknown
    }

    # Handle Additional Details if OrderIdIfood is provided
    if order["OrderIdIfood"] is not None:
        order_error_details = error_repository.find_by_number_ifood(order["OrderIdIfood"])
        if order_error_details:
            try:
                json_error = {
                    "OrderPrecode": order["OrderPrecode"],
                    "CodeError": order_error_details["cancel_code"],
                    "Message": order_error_details["cancel_reason"],
                    "Date": order["UpdateData"],
                    "SystemError": "IFOOD",
                    "Meaning": order_error_details["cancel_code_description"],
                }
            except Exception as error:
                logger.error(f"Erro na captura dos dados de cancelamento de chamado da tabela Ifood error: {error}")
    error_repository.insert(json_error)


def process_janis_data(api_order):
    erp_number = api_order["OrderERP"]
    if int(erp_number) < 10:
        return None

    janis_api_order = janis_service.find_order(erp_number)
    if not janis_api_order:
        return None

    janis_db = janis_repository.find_by_ecommerce_id(janis_api_order["data"]["ecommerceId"])
    if janis_db:
        janis_id = janis_db["id"]
        janis_repository.update(janis_id, janis_api_order)
    else:
        janis_id = janis_repository.insert(janis_api_order)

    return janis_id


def process_vtex_data(precode_order):
    erp_number = precode_order["OrderERP"]
    if int(erp_number) < 10:
        return None

    vtex_order = vtex_service.find_order(erp_number)
    if not vtex_order:
        return None

    db_order = vtex_repository.find_by_erp_number(erp_number)
    if db_order:
        vtex_repository.update(vtex_order)
        vtex_id = db_order["id"]
    else:
        vtex_id = vtex_repository.insert(vtex_order)

    return vtex_id


def process_ifood_data(precode_order, token):
    try:
        id_pedido = precode_order["OrderPartner"]
    except Exception:
        return None

    ifood_order = ifood_service.get_orders_details(id_pedido, token)
    if not ifood_order:
        return None

    order_id = ifood_order["idPedido"]

    return order_id


def process_order(api_order, token):

    if api_order["Partner"] != "iFood":
        return
    logger.info(f'Start Processing Order: {api_order}')

    api_order["janisId"] = None
    api_order["vtexId"] = None

    api_order["janisId"] = process_janis_data(api_order)
    api_order["vtexId"] = process_vtex_data(api_order)

    process_precode_data(api_order, token)

    process_errors(api_order)

    logger.info(f'Finish Processing Order: {api_order["OrderPrecode"]}')


def process_precode_data(api_order, token):

    db_order = order_repository.find_by_number_precode(api_order["OrderPrecode"])
    add_store_data(api_order)
    if db_order:
        logger.info('perform update')
        order_repository.update(api_order)

    else:
        logger.info('perform create')
        order_repository.insert(api_order)

    return db_order


def process_errors(api_order):
    if CANCELLED_ERP_STATUS in api_order["Status"] or CANCELLED_MKT_STATUS in api_order["Status"]:
        consult = error_repository.find_by_number_precode(api_order["OrderPrecode"])
        if consult:
            return

        add_system_error_data(api_order)

        order_log = precode_service.find_order_log(api_order["OrderPrecode"])
        order_log = utils.remove_duplicated_items(order_log)

        if CANCELLED_ERP_STATUS in api_order["Status"]:
            process_erp_error(api_order, order_log)
            return

        if CANCELLED_MKT_STATUS in api_order["Status"]:
            process_marketplace_error(api_order, order_log)
            return


def load_carrefour_orders(day=None):
    logger.info('load_carrefour_orders in service Started:')

    start_datetime, end_datetime = calc_initial_final_date(day)

    orders = precode_service.get_all_orders_precode(start_datetime, end_datetime)
    orders = utils.remove_duplicated_items(orders)

    logger.info(f'Orders found unique: {len(orders)}')

    # Getting/generating Ifood Mercado API Token
    token = oauthIFoodClient.get_token()

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = []  # List to store the futures

        # Submit tasks to the thread pool and store the futures in the list
        for order in orders:
            if order["OrderIdIfood"] == 'Null':
                order["OrderIdIfood"] = process_ifood_data(order, token)
            future = executor.submit(process_order, order, token)
            futures.append(future)

        # Wait for all tasks to complete
        for future in futures:
            try:
                future.result()
            except Exception as e:
                logger.error(f"Fail process order: {e}", exc_info=True)

    logger.info('load_carrefour_orders in service Finish:')



def calc_initial_final_date(day=None):

    end_datetime = datetime.now()
    start_datetime = end_datetime - timedelta(minutes=1)

    if day:
        start_datetime = datetime(2023, MONTH, day, HOUR_INI, MINUTE_INI, SECOND_INI)
        end_datetime = datetime(2023, MONTH, day, HOUR_FIM, MINUTE_FIM, SECOND_FIM)

    return start_datetime, end_datetime
