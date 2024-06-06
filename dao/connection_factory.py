import psycopg2
from psycopg2 import pool

from util import properties_reader


class DatabaseConnectionFactory:
    def __init__(self):
        section_name = 'DataBase'

        self.database_name = properties_reader.property_value(section_name, 'database_name')
        self.user = properties_reader.property_value(section_name, 'user')
        self.password = properties_reader.property_value(section_name, 'password')
        self.host = properties_reader.property_value(section_name, 'host')
        self.port = properties_reader.property_value(section_name, 'port')
        self.max_connections = properties_reader.property_value(section_name, 'max_connections')
        self.connection_pool = self.create_connection_pool()

    def create_connection_pool(self):
        return psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=self.max_connections,
            dbname=self.database_name,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )

    def get_connection(self):
        return self.connection_pool.getconn()

    def release_connection(self, connection):
        self.connection_pool.putconn(connection)

    def close_all_connections(self):
        self.connection_pool.closeall()


connection_factory = DatabaseConnectionFactory()
