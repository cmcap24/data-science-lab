import logging

import pandas as pd

from pymongo import MongoClient

logging.basicConfig(level=logging.INFO)


from utils.database_utils.database_config import mongodb_config


class MongoDBHandler(object):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            # create new instance if instance does not exist
            cls._instance = object.__new__(cls)
            # read in postgres config
            config_string = mongodb_config()
            # try connection
            try:
                logging.info('Connecting to MongoDB database...')
                client = MongoDBHandler._instance.client = MongoClient(config_string)
            except Exception as error:
                logging.info(f'Error: connection not established {error}')
                MongoDBHandler._instance = None
            else:
                logging.info(f'connection established')
        return cls._instance

    def __init__(self, db_name=None):
        self.client = self._instance.client
        self.db_name = db_name
        if self.db_name:
            self.db = self.client[self.db_name]

    def query(self, query_string):
        return query_string

    def list_databases(self):
        return self.client.list_database_names()

    def create_database(self, db_name):
        database_list = self.list_databases()
        if db_name not in database_list:
            self.db = self.client[db_name]

    def drop_database(self, db_name):
        self.client.drop_database(db_name)

    def list_collections(self):
        return self.db.list_collection_names()

    def create_collection(self, collection_name):
        collections = self.list_collections()
        if collections:
            if collection_name in collections:
                logging.info('collection already exists')
                return
        collection = self.db[collection_name]
        return None

    def drop_collection(self, collection_name):
        self.db[collection_name].drop()

    def insert_values(self, collection_name, data):
        if isinstance(data, pd.DataFrame):
            data = data.to_dict('records')
        try:
            self.db[collection_name].insert_many(data)
        except Exception as e:
            logging.info(f'error: {e}')

    def __del__(self):
        self.client.close()
