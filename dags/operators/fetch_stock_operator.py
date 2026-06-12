from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import yfinance as yf
import pandas as pd
from datetime import datetime

#Класс оператора для загрузки данных с Yahoo Finance
class FetchStockDataOperator(BaseOperator):
		@apply_defaults
		def __init__(self, tickers, start_date='2020-01-01', end_date=None, **kwargs):
				
				#Вызываем конструктор родителя (BaseOperator)
				super().__init__(**kwargs)

				#Сохраняем переданные параметры в атрибуты объекта
				self.tickers = tickers
				self.start_date = start_date

				#Если end_date не задан, используем сегодняшнюю дату
				self.end_date = end_date or datetime.today().strftime('%Y-%m-%d')

		def pre_execute(self, context):
				self.log.info(f"Начинаем загрузку данных для {self.tickers}")

		def execute(self, context):
				all_data = []  #Список для сбора DataFrame всех тикеров
				for ticker in self.tickers:
						self.log.info(f"Загружаем {ticker}...")

						#Скачиваем данные с Yahoo Finance (OHLCV)
						df = yf.download(ticker, start=self.start_date, end=self.end_date, progress=False)
						df.reset_index(inplace=True)             # Делаем дату обычной колонкой
						df['Ticker'] = ticker                    # Добавляем колонку с именем тикера
						all_data.append(df)

				#Объединяем все DataFrame в один
				combined = pd.concat(all_data, ignore_index=True)

				#Приводим названия колонок к нижнему регистру (для единообразия)
				combined.columns = [c.lower() for c in combined.columns]
				self.log.info(f"Загружено {len(combined)} строк")

				#Возвращаем данные в виде списка словарей (это попадёт в XCom)
				return combined.to_dict('records')

		def post_execute(self, context, result):
				self.log.info("Загрузка завершена")