from airflow.hooks.base import BaseHook   
from pymongo import MongoClient           

class MongoHook(BaseHook):
		#Идентификатор подключения в Airflow
		def __init__(self, conn_id='mongo_default', *args, **kwargs):
				super().__init__(*args, **kwargs)
				self.conn_id = conn_id

		#Возвращает объект подключения
		def get_conn(self):
				self.log.info(f"Подключение к MongoDB через {self.conn_id}")

				#Получаем настройки подключения из Airflow Connections
				conn = self.get_connection(self.conn_id)

				#Формируем URI подключения (логин, пароль, хост, порт)
				uri = f"mongodb://{conn.login}:{conn.password}@{conn.host}:{conn.port}/"
				client = MongoClient(uri)
				return client

		#Возвращаем объект базы данных
		def get_db(self, db_name):
				return self.get_conn()[db_name]