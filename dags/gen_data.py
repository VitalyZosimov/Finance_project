from hooks.mongo_hook import MongoHook
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

hook = MongoHook()
db = hook.get_db('stockviz')

print('Генерация данных...')

tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
dates = pd.date_range(start='2020-01-01', end='2025-12-31', freq='D')

all_data = []
for ticker in tickers:
    base_price = np.random.uniform(100, 500)
    for date in dates:
        price = base_price + np.random.randn() * 5
        all_data.append({
            'date': date.strftime('%Y-%m-%d'),
            'ticker': ticker,
            'close': round(price, 2),
            'volume': np.random.randint(1000000, 20000000)
        })

db.stock_data.drop()
db.stock_data.insert_many(all_data)
print(f'Сгенерировано {len(all_data)} записей')