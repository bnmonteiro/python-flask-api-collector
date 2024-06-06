import logging
import psycopg2
from dao.connection_factory import connection_factory


def find_all():
    connection = connection_factory.get_connection()
    if connection is None:
        return

    try:
        cursor = connection.cursor()
        cursor.execute(f'SELECT * FROM orders.vtex_order;')
        rows = cursor.fetchall()
        return rows
    except psycopg2.Error:
        logging.error("Falha ao executar:", exc_info=True)
    finally:
        cursor.close()
        connection_factory.release_connection(connection)


def find_by_erp_number(erp_number):
    connection = connection_factory.get_connection()
    if connection is None:
        return
    try:
        cursor = connection.cursor()
        cursor.execute(f'SELECT * FROM orders.vtex_order where sequence_number = %s limit 1;', (erp_number,))
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


def insert(vtex_order):
    connection = connection_factory.get_connection()
    if connection is None:
        return
    try:
        cursor = connection.cursor()
        query = \
            "INSERT INTO orders.vtex_order " \
            "(order_id, sequence_number, status, status_description, market_place_order_id, seller_order_id, origin, " \
            "affiliate_id, sales_channel, merchant_name, order_group, workflow_is_in_error, value, api_updated_date, " \
            "api_created_date) " \
            "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;"

        data = (vtex_order["orderId"],
                vtex_order["sequence"],
                vtex_order["status"],
                vtex_order["statusDescription"],
                vtex_order["marketplaceOrderId"],
                vtex_order["sellerOrderId"],
                vtex_order["origin"],
                vtex_order["affiliateId"],
                vtex_order["salesChannel"],
                vtex_order["merchantName"],
                vtex_order["orderGroup"],
                vtex_order["workflowIsInError"],
                vtex_order["value"],
                vtex_order["lastChange"],
                vtex_order["creationDate"]
                )

        cursor.execute(query, data)
        generated_id = cursor.fetchone()[0]
        connection.commit()
        return generated_id

    except psycopg2.Error as e:
        logging.error(f'Falha insert vtex order: {vtex_order}', exc_info=True)
    finally:
        cursor.close()
        connection_factory.release_connection(connection)


def update(vtex_order):

    connection = connection_factory.get_connection()
    if connection is None:
        return
    try:
        cursor = connection.cursor()
        update_query = \
            "UPDATE orders.vtex_order SET order_id=%s, status=%s, status_description=%s, " \
            "market_place_order_id=%s, seller_order_id=%s, origin=%s, affiliate_id=%s, sales_channel=%s, " \
            "merchant_name=%s, order_group=%s, workflow_is_in_error=%s, value=%s, api_updated_date=%s, " \
            "api_created_date=%s, updated_date=now() WHERE sequence_number=%s;"

        data = (vtex_order["orderId"],
                vtex_order["status"],
                vtex_order["statusDescription"],
                vtex_order["marketplaceOrderId"],
                vtex_order["sellerOrderId"],
                vtex_order["origin"],
                vtex_order["affiliateId"],
                vtex_order["salesChannel"],
                vtex_order["merchantName"],
                vtex_order["orderGroup"],
                vtex_order["workflowIsInError"],
                vtex_order["value"],
                vtex_order["lastChange"],
                vtex_order["creationDate"],
                vtex_order["sequence"]
                )

        cursor.execute(update_query, data)
        connection.commit()

    except Exception as e:
        logging.error("Falha ao executar update vtex order: ", exc_info=True)
    finally:
        cursor.close()
        connection_factory.release_connection(connection)
