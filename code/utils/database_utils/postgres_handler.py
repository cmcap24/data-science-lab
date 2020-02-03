import logging
logging.basicConfig(level=logging.INFO)

import psycopg2

from utils.database_utils.database_config import postgres_config


class PostgresHandler(object):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            # create new instance if instance does not exist
            cls._instance = object.__new__(cls)
            # read in postgres config
            config = postgres_config()
            if kwargs:
                config['database'] = kwargs['db_name']
            # try connection
            try:
                logging.info('Connecting to PostgreSQL database...')
                connection = PostgresHandler._instance.connection = psycopg2.connect(**config)
                cursor = PostgresHandler._instance.cursor = connection.cursor()
                PostgresHandler._instance.connection.autocommit = True
                cursor.execute('SELECT VERSION()')
                db_version = cursor.fetchone()
            except Exception as error:
                logging.info(f'Error: connection not established {error}')
                PostgresHandler._instance = None
            else:
                logging.info(f'connection established\n{db_version[0]}')
        return cls._instance

    def __init__(self, db_name=None):
        self.connection = self._instance.connection
        self.cursor = self._instance.cursor
        self.db_name = db_name

    def query(self, query_string):
        try:
            result = self.cursor.execute(query_string)
        except Exception as error:
            logging.info(f'error executing query {query_string}: {error}')
        else:
            return result

    def list_databases(self):
        self.query("SELECT datname FROM pg_database;")
        return [db[0] for db in self.cursor]

    def create_database(self, db_name):
        self.query(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db_name}'")
        exists = self.cursor.fetchone()
        if not exists:
            self.query(f'CREATE DATABASE {db_name}')

    def drop_database(self, db_name):
        self.query(f'DROP DATABASE IF EXISTS {db_name}')

    def list_tables(self):
        self.query("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' """)
        if self.cursor:
            return [table[0] for table in self.cursor.fetchall()]

    def create_table(self, table_name, table_schema):
        # table schema must be list of dictionaries of the form
        # {'field_name': str, 'field_type': str, 'is_primary_key': bool, 'is_nullable': bool}
        tables = self.list_tables()
        if tables:
            if table_name in tables:
                logging.info('table already exists')
                return

        query_body = ''
        for field_dict in table_schema:
            query_line_end = ''
            if field_dict['is_primary_key']:
                query_line_end += 'PRIMARY KEY,'
            else:
                if not field_dict['is_nullable']:
                    query_line_end += 'NOT NULL,'
            query_body += "{field_name} {field_type} ".format(**field_dict) + query_line_end
        query_body = query_body.rstrip(',')
        query_string = f'''CREATE TABLE {table_name} ({query_body})'''
        self.query(query_string)

    def drop_table(self, table_name):
        self.query(f'DROP TABLE IF EXISTS {table_name}')

    def insert_values(self, table_name, column_names, data):
        column_string = str(tuple(column_names)).replace("'", "")
        insert_query_string = f'''INSERT INTO {table_name} {column_string} VALUES '''
        data_string = b','.join(self.cursor.mogrify("%s", (val, )) for val in data).decode("utf-8")
        insert_query_string += data_string
        self.query(insert_query_string)

    def __del__(self):
        self.connection.close()
        self.cursor.close()
