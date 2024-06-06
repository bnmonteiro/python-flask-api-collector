import logging

import psycopg2
from dao.connection_factory import connection_factory


def find_all():
    connection = connection_factory.get_connection()
    if connection is None:
        return

    try:
        cursor = connection.cursor()
        cursor.execute(f'SELECT * FROM orders.precode_order;')
        rows = cursor.fetchall()
        return rows
    except psycopg2.Error:
        logging.error("Falha ao executar:", exc_info=True)
    finally:
        cursor.close()
        connection_factory.release_connection(connection)


def find_by_number_precode(precode_number):
    connection = connection_factory.get_connection()
    if connection is None:
        return
    try:
        cursor = connection.cursor()
        cursor.execute(f'SELECT * FROM orders.precode_order where precode_number = %s limit 1;', (precode_number,))
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


def insert(order_precode):
    connection = connection_factory.get_connection()
    if connection is None:
        return
    try:
        cursor = connection.cursor()
        query = "INSERT INTO orders.precode_order" \
                "(precode_number, erp_number, partner_number, specific_number, document_number, status, partner," \
                " branch, total_order_value, updated_date, created_date, store_id, janis_id, vtex_id, ifood_id, bandeira) " \
                "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

        data = (order_precode["OrderPrecode"],
                order_precode["OrderERP"],
                order_precode["OrderPartner"],
                order_precode["OrderSpecific"],
                order_precode["Document"],
                order_precode["Status"],
                order_precode["Partner"],
                order_precode["Branch"],
                order_precode["TotalOrderValue"],
                order_precode["UpdateData"],
                order_precode["CreateDate"],
                order_precode["storeId"],
                order_precode["janisId"],
                order_precode["vtexId"],
                order_precode["OrderIdIfood"],
                order_precode["bandeira"]
                )

        cursor.execute(query, data)
        connection.commit()

    except psycopg2.Error as e:
        logging.error(f'Falha insert pedido_precode: {order_precode} - {e}', exc_info=True)
    finally:
        cursor.close()
        connection_factory.release_connection(connection)


def update(precode_order):
    connection = connection_factory.get_connection()
    if connection is None:
        return
    try:
        cursor = connection.cursor()
        update_query = "UPDATE orders.precode_order SET erp_number=%s, partner_number=%s, specific_number=%s, " \
                "document_number=%s, status=%s, partner=%s, branch=%s, total_order_value=%s, updated_date=%s, " \
                "created_date = %s, janis_id = %s, vtex_id = %s, store_id = %s, bandeira = %s " \
                       "WHERE precode_number=%s;"

        data = (
                precode_order["OrderERP"],
                precode_order["OrderPartner"],
                precode_order["OrderSpecific"],
                precode_order["Document"],
                precode_order["Status"],
                precode_order["Partner"],
                precode_order["Branch"],
                precode_order["TotalOrderValue"],
                precode_order["UpdateData"],
                precode_order["CreateDate"],
                precode_order["janisId"],
                precode_order["vtexId"],
                precode_order["storeId"],
                precode_order["bandeira"],
                precode_order["OrderPrecode"]
        )

        cursor.execute(update_query, data)
        connection.commit()

    except psycopg2.Error as e:
        logging.error(f"Falha ao executar: {e}", exc_info=True)
    finally:
        cursor.close()
        connection_factory.release_connection(connection)
