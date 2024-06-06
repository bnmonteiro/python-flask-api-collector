import asyncio
import logging
import threading

import schedule

from service.collect_order_service import load_carrefour_orders
from service.db_session_service import kill_tableau_db_sessions
from service.janis_sync_service import check_orders_status
from service.loggin_service import compact_yesterday_log_file

logger = logging.getLogger(__name__)


def zip_log_file_task():
    compact_yesterday_log_file()
    logger.info('compact_yesterday_log_file')


def load_carrefour_orders_task():
    logger.info("Orders trigger start...")
    order_thread = threading.Thread(target=load_carrefour_orders)
    order_thread.start()
    logger.info("Orders trigger finish...")


def kill_tableau_db_sessions_task():
    logger.info("db_sessions trigger start...")
    tableau_thread = threading.Thread(target=kill_tableau_db_sessions)
    tableau_thread.start()
    logger.info("db_sessions trigger finish...")


def update_janis_status_task():
    logger.info("Janis trigger start...")
    sync_thread = threading.Thread(target=check_orders_status)
    sync_thread.start()
    logger.info("Janis trigger finish...")


def run_schedule():
    schedule.run_pending()
    asyncio.get_event_loop().call_later(1, run_schedule)


def execute_task():

    ##first execution
    load_carrefour_orders_task()
    kill_tableau_db_sessions_task()
    update_janis_status_task()

    ##Schedules
    schedule.every(1).minutes.do(load_carrefour_orders_task)
    schedule.every(1).minutes.do(kill_tableau_db_sessions_task)
    schedule.every(5).minutes.do(update_janis_status_task)

    # Executar o agendamento usando asyncio
    loop = asyncio.get_event_loop()
    loop.call_soon(run_schedule)
    loop.run_forever()



