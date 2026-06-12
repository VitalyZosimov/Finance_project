from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
import numpy as np

class CalculateMetricsOperator(BaseOperator):
		@apply_defaults
		def __init__(self, weights, initial_cash=100000, **kwargs):
				super().__init__(**kwargs)
				self.weights = weights          # Словарь весов активов в портфеле
				self.initial_cash = initial_cash  # Начальный капитал

		def execute(self, context):
				# Получаем данные из FetchStockDataOperator через XCom
				ti = context['ti']
				raw_records = ti.xcom_pull(task_ids='fetch_stock_data')
				# Преобразуем в pandas DataFrame
				df = pd.DataFrame(raw_records)
				df['date'] = pd.to_datetime(df['date'])
				df = df.sort_values(['ticker', 'date'])  # Сортируем по тикеру и дате

				# 1. Рассчитываем рыночные метрики для каждого тикера
				df = self._add_market_metrics(df)

				# 2. Рассчитываем портфельные метрики
				portfolio_df = self._calc_portfolio_metrics(df)

				# Возвращаем словарь с двумя наборами метрик (оба попадут в XCom)
				return {
						'ticker_metrics': df.to_dict('records'),
						'portfolio_metrics': portfolio_df.to_dict('records')
				}

		# Вспомогательный метод для рыночных метрик
		def _add_market_metrics(self, df):
				# Ежедневная процентная доходность (close сегодня / close вчера - 1)
				df['daily_return'] = df.groupby('ticker')['close'].pct_change()

				# Логарифмическая доходность (более точная для временных рядов)
				df['log_return'] = np.log(df['close'] / df.groupby('ticker')['close'].shift(1))

				# 20-дневная волатильность, годовая (умножение на sqrt(252))
				df['volatility_20d'] = df.groupby('ticker')['daily_return'].transform(
						lambda x: x.rolling(20, min_periods=1).std() * np.sqrt(252))
				
				# Функция для расчёта RSI за 14 дней
				def rsi(series, period=14):
						delta = series.diff()
						gain = delta.clip(lower=0)
						loss = -delta.clip(upper=0)
						avg_gain = gain.rolling(period, min_periods=1).mean()
						avg_loss = loss.rolling(period, min_periods=1).mean()
						rs = avg_gain / avg_loss
						return 100 - (100 / (1 + rs))
				df['rsi_14'] = df.groupby('ticker')['close'].transform(rsi)

				# Простые скользящие средние за 20 и 50 дней
				df['sma_20'] = df.groupby('ticker')['close'].transform(lambda x: x.rolling(20, min_periods=1).mean())
				df['sma_50'] = df.groupby('ticker')['close'].transform(lambda x: x.rolling(50, min_periods=1).mean())

				# Средний объём за 20 дней и отношение текущего объёма к среднему
				df['avg_volume_20d'] = df.groupby('ticker')['volume'].transform(lambda x: x.rolling(20, min_periods=1).mean())
				df['volume_ratio'] = df['volume'] / df['avg_volume_20d']

				# Просадка (drawdown) – относительное падение от исторического максимума
				df['cummax_close'] = df.groupby('ticker')['close'].cummax()      # Текущий максимум
				df['drawdown'] = (df['close'] - df['cummax_close']) / df['cummax_close']  # Просадка
				df['max_drawdown'] = df.groupby('ticker')['drawdown'].cummin()   # Максимальная просадка на дату
				df.drop('cummax_close', axis=1, inplace=True)  # Удаляем временную колонку
				return df

		# Расчёт портфельных метрик (доходность, стоимость, VaR)
		def _calc_portfolio_metrics(self, df):
				#Строим сводную таблицу: строки – даты, столбцы – тикеры, значения – daily_return
				pivot = df.pivot(index='date', columns='ticker', values='daily_return')
				#Оставляем тикеры, для которых есть веса и данные
				avail = [t for t in self.weights if t in pivot.columns]
				if not avail:
						raise ValueError("Нет совпадений по тикерам")
				
				#Массив весов
				w = np.array([self.weights[t] for t in avail])

				#Доходность портфеля в каждый день
				port_returns = pivot[avail].dot(w)

				#Накопленная стоимость портфеля (начальный капитал * накопленный множитель)
				port_value = self.initial_cash * (1 + port_returns).cumprod()

				#Текущий исторический максимум стоимости
				running_max = port_value.cummax()

				#Просадка портфеля
				drawdown = (port_value - running_max) / running_max

				#Исторический VaR 95%: 5-й процентиль доходности за окно 252 дня
				var_95 = port_returns.rolling(252, min_periods=20).quantile(0.05)

				#CVaR – среднее доходности в худших 5% (для простоты копируем VaR, можно улучшить)
				cvar_95 = var_95.copy()

				#Формируем итоговый DataFrame портфельных метрик
				result = pd.DataFrame({
						'date': pivot.index,
						'portfolio_value': port_value,
						'daily_return': port_returns,
						'cumulative_return': port_value / self.initial_cash - 1,  # Относительная доходность
						'drawdown': drawdown,
						'max_drawdown': drawdown.cummin(),
						'var_95': var_95,
						'cvar_95': cvar_95
				}).reset_index(drop=True)
				
				# Добавляем колонки с весами (для информации)
				for t in avail:
						result[f'weight_{t}'] = self.weights[t]
				return result