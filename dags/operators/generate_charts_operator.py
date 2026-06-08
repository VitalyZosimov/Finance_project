from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from dags.hooks.mongo_hook import MongoHook
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

class GenerateChartsOperator(BaseOperator):
		@apply_defaults
		def __init__(self, output_dir='output', **kwargs):
				super().__init__(**kwargs)
				self.output_dir = output_dir   #HTML-графики

		def pre_execute(self, context):
				os.makedirs(self.output_dir, exist_ok=True)   

		def execute(self, context):
				#Подключаемся к MongoDB
				hook = MongoHook()
				db = hook.get_db('stockviz')

				#Загружаем портфельные данные (коллекция portfolio_snapshots)
				portfolio = pd.DataFrame(list(db.portfolio_snapshots.find({}, {'_id':0})))
				if portfolio.empty:
						self.log.warning("Нет портфельных данных")
						return
				portfolio['date'] = pd.to_datetime(portfolio['date'])
				portfolio = portfolio.sort_values('date')

				#График 1: стоимость портфеля по дням
				fig1 = px.line(portfolio, x='date', y='portfolio_value', title='Стоимость портфеля (из MongoDB)')
				fig1.write_html(os.path.join(self.output_dir, 'portfolio_value.html'))

				#График 2: просадка портфеля (залитая область)
				fig2 = go.Figure()
				fig2.add_trace(go.Scatter(x=portfolio['date'], y=portfolio['drawdown']*100, fill='tozeroy', name='Просадка, %'))
				fig2.update_layout(title='Просадка портфеля')
				fig2.write_html(os.path.join(self.output_dir, 'drawdown.html'))

				#График 3: гистограмма дневной доходности с линией VaR
				fig3 = px.histogram(portfolio, x='daily_return', nbins=50, title='Распределение доходности')
				last_var = portfolio['var_95'].iloc[-1] if not portfolio['var_95'].isna().all() else 0
				fig3.add_vline(x=last_var, line_dash='dash', line_color='red', annotation_text=f'VaR 95%: {last_var:.2%}')
				fig3.write_html(os.path.join(self.output_dir, 'hist_return.html'))

				#График 4: тепловая карта корреляций между тикерами
				ticker_data = pd.DataFrame(list(db.ticker_metrics.find({}, {'_id':0, 'date':1, 'ticker':1, 'daily_return':1})))
				if not ticker_data.empty:
						pivot = ticker_data.pivot(index='date', columns='ticker', values='daily_return')
						corr = pivot.corr()
						fig4 = px.imshow(corr, text_auto=True, title='Корреляция активов')
						fig4.write_html(os.path.join(self.output_dir, 'correlation.html'))

				self.log.info(f"Графики сохранены в {self.output_dir}")