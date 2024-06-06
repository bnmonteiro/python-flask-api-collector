#!/usr/bin/python3

from service.loggin_service import configure_logging
from task import load_api_task

configure_logging()
load_api_task.execute_task()

