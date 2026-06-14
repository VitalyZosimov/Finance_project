from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.mongo_hook import MongoHook


class EtlToMongoOperator(BaseOperator):
    @apply_defaults
    def __init__(self, mongo_conn_id='mongo_default', db_name='stockviz', **kwargs):
        super().__init__(**kwargs)
        self.mongo_conn_id = mongo_conn_id
        self.db_name = db_name

    def execute(self, context):
        ti = context['ti']
        metrics = ti.xcom_pull(task_ids='calculate_metrics')

        if not metrics:
            raise ValueError("Нет данных от calculate_metrics")

        hook = MongoHook(conn_id=self.mongo_conn_id)
        db = hook.get_db(self.db_name)

        db.ticker_metrics.drop()
        db.portfolio_snapshots.drop()

        if metrics['ticker_metrics']:
            db.ticker_metrics.insert_many(metrics['ticker_metrics'])
        if metrics['portfolio_metrics']:
            db.portfolio_snapshots.insert_many(metrics['portfolio_metrics'])

        self.log.info(f"Загружено {len(metrics['ticker_metrics'])} записей")