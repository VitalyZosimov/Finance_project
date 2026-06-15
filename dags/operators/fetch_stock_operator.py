from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import yfinance as yf
import pandas as pd
from datetime import datetime


class FetchStockDataOperator(BaseOperator):
    @apply_defaults
    def __init__(self, tickers, start_date='2020-01-01', end_date=None, **kwargs):
        super().__init__(**kwargs)
        self.tickers = tickers
        self.start_date = start_date
        self.end_date = end_date or datetime.today().strftime('%Y-%m-%d')

    def execute(self, context):
        all_data = []
        for ticker in self.tickers:
            self.log.info('Downloading {}...'.format(ticker))
            df = yf.download(ticker, start=self.start_date, end=self.end_date, progress=False)
            df.reset_index(inplace=True)
            df['Ticker'] = ticker
            all_data.append(df)

        combined = pd.concat(all_data, ignore_index=True)
        combined.columns = [c.lower() for c in combined.columns]
        self.log.info('Loaded {} rows'.format(len(combined)))
        return combined.to_dict('records')