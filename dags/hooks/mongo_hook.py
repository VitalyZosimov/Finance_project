from airflow.hooks.base import BaseHook
from pymongo import MongoClient


class MongoHook(BaseHook):
    def __init__(self, conn_id='mongo_default', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id

    def get_conn(self):
        self.log.info(f"Подключение к MongoDB через {self.conn_id}")
        conn = self.get_connection(self.conn_id)
        uri = f"mongodb://{conn.login}:{conn.password}@{conn.host}:{conn.port}/"
        return MongoClient(uri)

    def get_db(self, db_name):
        return self.get_conn()[db_name]