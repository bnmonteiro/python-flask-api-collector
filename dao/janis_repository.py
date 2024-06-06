import logging
import psycopg2
from dao.connection_factory import connection_factory
from datetime import datetime, timedelta


def find_all():
    connection = connection_factory.get_connection()
    if connection is None:
        return

    try:
        cursor = connection.cursor()
        cursor.execute(f'SELECT * FROM orders.janis_order;')
        rows = cursor.fetchall()
        return rows
    except psycopg2.Error:
        logging.error("Falha ao executar:", exc_info=True)
    finally:
        cursor.close()
        connection_factory.release_connection(connection)


def find_all_pending_status():

    end_datetime = datetime.now()
    start_datetime = end_datetime - timedelta(hours=4)
    start_datetime = start_datetime.strftime("%Y-%m-%d %H:%M:%S")

    connection = connection_factory.get_connection()
    if connection is None:
        return

    try:
        cursor = connection.cursor()
        query = " SELECT j.id, p.erp_number , j.status_id, j.status_description, p.status  FROM orders.janis_order j" \
                " join orders.precode_order p  on p.janis_id = j.id" \
                " where status_id in (5,10)" \
                " and p.status = 'Em processo de separação e faturamento.'" \
                " and p.created_date >= %s"

        cursor.execute(query, (start_datetime,))

        rows = cursor.fetchall()
        return rows
    except psycopg2.Error:
        logging.error("Falha ao executar:", exc_info=True)
    finally:
        cursor.close()
        connection_factory.release_connection(connection)


def find_by_id(janis_id):
    connection = connection_factory.get_connection()
    if connection is None:
        return
    try:
        cursor = connection.cursor()
        cursor.execute(f'SELECT * FROM orders.janis_order where id = %s limit 1;', (janis_id,))
        row = cursor.fetchone()
        if row:

            column_names = [desc[0] for desc in cursor.description]
            row_dict = {column_names[i]: row[i] for i in range(len(column_names))}

            return row_dict
        else:
            return None
    except psycopg2.Error:
        logging.error("Falha ao executar janis find by id:", exc_info=True)
    finally:
        cursor.close()
        connection_factory.release_connection(connection)


def find_by_ecommerce_id(ecommerce_id):
    connection = connection_factory.get_connection()
    if connection is None:
        return
    try:
        cursor = connection.cursor()
        cursor.execute(f'SELECT * FROM orders.janis_order where ecommerce_id = %s limit 1;', (ecommerce_id,))
        row = cursor.fetchone()
        if row:
            # Get the column names
            column_names = [desc[0] for desc in cursor.description]
            # Build a dictionary with column names as keys and row values as values
            row_dict = {column_names[i]: row[i] for i in range(len(column_names))}
            return row_dict
        else:
            return None
    except psycopg2.Error:
        logging.error("Falha ao executar:", exc_info=True)
    finally:
        cursor.close()
        connection_factory.release_connection(connection)


def insert(janis_order):
    connection = connection_factory.get_connection()
    if connection is None:
        return
    try:
        cursor = connection.cursor()
        query = \
            "INSERT INTO orders.janis_order (code, message, ecommerce_id, cart_id, sales_channel, store, status_id, " \
            "status_description, value, api_created_date) " \
            "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;"

        data = (janis_order["code"],
                janis_order["message"],
                janis_order["data"]["ecommerceId"],
                janis_order["data"]["cartId"],
                janis_order["data"]["salesChannel"],
                janis_order["data"]["store"],
                janis_order["data"]["status"]["id"],
                janis_order["data"]["status"]["description"],
                janis_order["data"]["value"],
                janis_order["data"]["creationDate"]
                )

        cursor.execute(query, data)
        generated_id = cursor.fetchone()[0]
        connection.commit()
        return generated_id

    except Exception as e:
        logging.error(f'Falha insert janis order: {janis_order}', exc_info=True)
    finally:
        cursor.close()
        connection_factory.release_connection(connection)


def update(janis_id, janis_api_order):

    connection = connection_factory.get_connection()
    if connection is None:
        return
    try:
        cursor = connection.cursor()
        update_query = \
            "UPDATE orders.janis_order SET code=%s, message=%s, ecommerce_id=%s, cart_id=%s, sales_channel=%s, " \
            "store=%s, status_id=%s, status_description=%s, value=%s, api_created_date=%s, updated_date=now(), " \
            "previous_status = status_description " \
            "WHERE id=%s;"

        data = (janis_api_order["code"],
                janis_api_order["message"],
                janis_api_order["data"]["ecommerceId"],
                janis_api_order["data"]["cartId"],
                janis_api_order["data"]["salesChannel"],
                janis_api_order["data"]["store"],
                janis_api_order["data"]["status"]["id"],
                janis_api_order["data"]["status"]["description"],
                janis_api_order["data"]["value"],
                janis_api_order["data"]["creationDate"],
                janis_id
                )

        cursor.execute(update_query, data)
        connection.commit()

    except Exception as e:
        logging.error("Falha ao executar update janis order: ", exc_info=True)
    finally:
        cursor.close()
        connection_factory.release_connection(connection)
