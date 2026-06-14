from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
import numpy as np


class CalculateMetricsOperator(BaseOperator):
    @apply_defaults
    def __init__(self, weights, initial_cash=100000, **kwargs):
        super().__init__(**kwargs)
        self.weights = weights
        self.initial_cash = initial_cash

    def execute(self, context):
        ti = context['ti']
        raw_records = ti.xcom_pull(task_ids='fetch_stock_data')
        df = pd.DataFrame(raw_records)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(['ticker', 'date'])

        # Простой расчёт метрик
        df['daily_return'] = df.groupby('ticker')['close'].pct_change()
        df['volatility_20d'] = df.groupby('ticker')['daily_return'].transform(
            lambda x: x.rolling(20, min_periods=1).std() * np.sqrt(252))

        # Расчёт портфеля
        pivot = df.pivot(index='date', columns='ticker', values='daily_return')
        avail = [t for t in self.weights if t in pivot.columns]
        w = np.array([self.weights[t] for t in avail])
        port_returns = pivot[avail].dot(w)
        port_value = self.initial_cash * (1 + port_returns).cumprod()

        result = pd.DataFrame({
            'date': pivot.index,
            'portfolio_value': port_value,
            'daily_return': port_returns,
        }).reset_index(drop=True)

        return {
            'ticker_metrics': df.to_dict('records'),
            'portfolio_metrics': result.to_dict('records')
        }