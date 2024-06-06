import logging

import psycopg2
from datetime import datetime, timedelta

import pytz

from util import properties_reader

logger = logging.getLogger(__name__)


def kill_tableau_db_sessions():
    logger.info("kill_tableau_db_sessions Starts")

    section_name = 'DataBase'
    db_config = {
        'host': properties_reader.property_value(section_name, 'host'),
        'database': properties_reader.property_value(section_name, 'database_name'),
        'user': properties_reader.property_value(section_name, 'usersa'),
        'password': properties_reader.property_value(section_name, 'passwordsa'),
    }

    # Limite de inatividade (em segundos) após o qual a sessão será encerrada
    limite_inatividade_segundos = 60  # 1 minuto

    try:
        # Conectando ao banco de dados
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()

        # Obtendo as sessões ativas
        cursor.execute("SELECT pid, state_change FROM pg_catalog.pg_stat_activity sa "
                       "where state = 'idle' and usename = 'tableau'")

        sessions = cursor.fetchall()

        logger.info(f'Found {len(sessions)} sessions idle tableau.')

        # Encerrando sessões inativas
        timezone = pytz.timezone('America/Sao_Paulo')
        now = datetime.now(timezone)
        for session in sessions:
            pid, state_change = session

            if now - state_change > timedelta(seconds=limite_inatividade_segundos):
                cursor.execute(f"SELECT pg_terminate_backend({pid});")
                logger.info(f"Sessão tableau {pid} encerrada.")

        connection.commit()
        cursor.close()
        connection.close()

        logger.info("kill_tableau_db_sessions Finish")

    except (Exception, psycopg2.Error) as error:
        logger.error("Erro ao conectar ao banco de dados:", error)



