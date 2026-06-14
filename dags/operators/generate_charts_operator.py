from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.mongo_hook import MongoHook
import pandas as pd
import plotly.express as px
import os


class GenerateChartsOperator(BaseOperator):
    @apply_defaults
    def __init__(self, output_dir='/opt/airflow/output', **kwargs):
        super().__init__(**kwargs)
        self.output_dir = output_dir

    def pre_execute(self, context):
        os.makedirs(self.output_dir, exist_ok=True)

    def execute(self, context):
        hook = MongoHook()
        db = hook.get_db('stockviz')

        portfolio = pd.DataFrame(list(db.portfolio_snapshots.find({}, {'_id': 0})))
        if portfolio.empty:
            self.log.warning("Нет портфельных данных")
            return

        portfolio['date'] = pd.to_datetime(portfolio['date'])
        fig = px.line(portfolio, x='date', y='portfolio_value', title='Стоимость портфеля')
        fig.write_html(os.path.join(self.output_dir, 'portfolio_value.html'))
        self.log.info(f"График сохранён в {self.output_dir}")