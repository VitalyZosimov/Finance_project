from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from dags.hooks.mongo_hook import MongoHook

class EtlToMongoOperator(BaseOperator):
		@apply_defaults
		def __init__(self, mongo_conn_id='mongo_default', db_name='stockviz', **kwargs):
				super().__init__(**kwargs)
				self.mongo_conn_id = mongo_conn_id   # ID подключения к MongoDB в Airflow
				self.db_name = db_name                # Имя базы данных в MongoDB

		def execute(self, context):
				#Получаем данные от задачи calculate_metrics (через XCom)
				ti = context['ti']
				metrics = ti.xcom_pull(task_ids='calculate_metrics')
				if not metrics:
						raise ValueError("Нет данных от calculate_metrics")
				ticker_metrics = metrics['ticker_metrics']     # Список словарей с метриками по тикерам
				portfolio_metrics = metrics['portfolio_metrics']  # Портфельные метрики

				#Создаём экземпляр нашего хука
				hook = MongoHook(conn_id=self.mongo_conn_id)

				#Получаем доступ к базе данных
				db = hook.get_db(self.db_name)

				#Очищаем старые коллекции (перезаписываем, чтобы не было дублей)
				db.ticker_metrics.drop()
				db.portfolio_snapshots.drop()

				#Вставляем новые документы
				if ticker_metrics:
						db.ticker_metrics.insert_many(ticker_metrics)   # Вставка многих записей
				if portfolio_metrics:
						db.portfolio_snapshots.insert_many(portfolio_metrics)

				self.log.info(f"Загружено {len(ticker_metrics)} в ticker_metrics, {len(portfolio_metrics)} в portfolio_snapshots")