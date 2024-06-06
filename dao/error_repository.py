import logging

import psycopg2
from dao.connection_factory import connection_factory


def find_all():
    connection = connection_factory.get_connection()
    if connection is None:
        return
    try:
        cursor = connection.cursor()
        cursor.execute(f'SELECT * FROM orders.error;')
        rows = cursor.fetchall()
        return rows
    except psycopg2.Error as e:
        logging.error("Erro ao executar a consulta:", exc_info=True)
    finally:
        cursor.close()
        connection_factory.release_connection(connection)


def find_by_number_precode(precode_number):
    connection = connection_factory.get_connection()
    if connection is None:
        return
    try:
        cursor = connection.cursor()
        cursor.execute(f'SELECT * FROM orders.order_error where precode_order_id = %s limit 1;', (precode_number,))
        row = cursor.fetchone()
        if row:
            # Get the column names
            column_names = [desc[0] for desc in cursor.description]
            # Build a dictionary with column names as keys and row values as values
            row_dict = {column_names[i]: row[i] for i in range(len(column_names))}
            return row_dict
        else:
            return None
    except psycopg2.Error as error:
        logging.error(f"Falha ao executar: {error}", exc_info=True)
    finally:
        cursor.close()
        connection_factory.release_connection(connection)


def find_by_number_ifood(order_id):
    connection = connection_factory.get_connection()
    if connection is None:
        return
    try:
        cursor = connection.cursor()
        cursor.execute(f'SELECT * FROM orders.ifood_order_error WHERE ifood_order_id = %s LIMIT 1;', (order_id,))
        row = cursor.fetchone()
        if row:
            # Get the column names
            column_names = [desc[0] for desc in cursor.description]
            # Build a dictionary with column names as keys and row values as values
            row_dict = {column_names[i]: row[i] for i in range(len(column_names))}
            return row_dict
        else:
            return None
    except psycopg2.Error as error:
        logging.error(f"Falha ao executar: {error}", exc_info=True)
    finally:
        cursor.close()
        connection_factory.release_connection(connection)


def insert(error):
    connection = connection_factory.get_connection()
    if connection is None:
        return
    try:
        cursor = connection.cursor()
        query = "INSERT INTO orders.order_error " \
                "(code, message, api_date, system_error, meaning, precode_order_id) " \
                "VALUES(%s, %s, %s, %s, %s, %s);"

        data = (
            error["CodeError"],
            error["Message"],
            error["Date"],
            error["SystemError"],
            error["Meaning"],
            error["OrderPrecode"]
        )

        cursor.execute(query, data)
        connection.commit()

    except psycopg2.Error as error:
        logging.error(f'Falha ao executar insert de erro:{error}', exc_info=True)
    finally:
        cursor.close()
        connection_factory.release_connection(connection)
