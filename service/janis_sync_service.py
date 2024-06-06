import concurrent
import logging
import time

from dao import janis_repository
from service import janis_api_service

logger = logging.getLogger(__name__)


def sync_janis_status(order):
    logger.info(f"[JANIS sync] processing {order}")
    janis_api_order = janis_api_service.find_order(order[1])
    if not janis_api_order:
        return
    api_status_id = janis_api_order["data"]["status"]["id"]
    db_status_id = order[2]
    if api_status_id != db_status_id:
        logger.info('[JANIS sync] found status diference. Performing status update')
        janis_repository.update(order[0], janis_api_order)


def check_orders_status():
    logger.info("[JANIS] check_orders_status start.")
    orders = janis_repository.find_all_pending_status()
    logger.info(f"found {len(orders)} janis orders")

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []  # List to store the futures

        # Submit tasks to the thread pool and store the futures in the list
        for order in orders:
            future = executor.submit(sync_janis_status, order)
            futures.append(future)

        # Wait for all tasks to complete
        for future in futures:
            try:
                future.result()
            except Exception as e:
                logger.error("[JANIS sync] Fail process order: ", exc_info=True)

    logger.info("[JANIS] check_orders_status finish.")



