import logging

import psycopg2
from dao.connection_factory import connection_factory
from psycopg2.extras import DictCursor


def find_all():
    connection = connection_factory.get_connection()
    if connection is None:
        return
    try:
        cursor = connection.cursor()
        cursor.execute(f'SELECT * FROM orders.store;')
        rows = cursor.fetchall()
        return rows
    except psycopg2.Error as e:
        logging.error("Falha ao executar:", exc_info=True)
    finally:
        cursor.close()
        connection_factory.release_connection(connection)


def find_by_code(code):
    connection = connection_factory.get_connection()
    if connection is None:
        return
    try:
        cursor = connection.cursor(cursor_factory=DictCursor)
        cursor.execute(f'SELECT * FROM orders.store where crf_id = %s limit 1;', (code,))

        row = cursor.fetchone()
        return row

    except psycopg2.Error as e:
        #print("Erro ao executar a consulta:", e)
        logging.error("Falha ao executar:", exc_info=True)
    finally:
        cursor.close()
        connection_factory.release_connection(connection)

