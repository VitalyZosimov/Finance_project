from hooks.mongo_hook import MongoHook
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests

print('1. Генерация stock_data...')
hook = MongoHook()
db = hook.get_db('stockviz')

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
print(f'stock_data: {len(all_data)}')

print('2. Расчёт портфеля...')
data = list(db.stock_data.find({}, {'_id': 0}))
df = pd.DataFrame(data)
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values(['ticker', 'date'])
df['daily_return'] = df.groupby('ticker')['close'].pct_change()

weights = {'AAPL': 0.3, 'MSFT': 0.3, 'GOOGL': 0.2, 'AMZN': 0.1, 'TSLA': 0.1}
pivot = df.pivot(index='date', columns='ticker', values='daily_return')

portfolio_returns = pd.Series(0, index=pivot.index)
for ticker, weight in weights.items():
    if ticker in pivot.columns:
        portfolio_returns += weight * pivot[ticker].fillna(0)

initial_cash = 100000
portfolio_value = initial_cash * (1 + portfolio_returns).cumprod()

result = pd.DataFrame({
    'date': portfolio_returns.index,
    'portfolio_return': portfolio_returns,
    'portfolio_value': portfolio_value
})

db.portfolio_metrics.drop()
db.portfolio_metrics.insert_many(result.to_dict('records'))
print(f'portfolio_metrics: {len(result)}')

print('3. MOEX...')
tickers_moex = ['SBER', 'GAZP', 'LKOH', 'ROSN', 'VTBR']
end_date = datetime.now()
start_date = end_date - timedelta(days=30)
moex_data = []

for ticker in tickers_moex:
    url = f'https://iss.moex.com/iss/engines/stock/markets/shares/boards/tqbr/securities/{ticker}/candles.json'
    params = {'from': start_date.strftime('%Y-%m-%d'), 'till': end_date.strftime('%Y-%m-%d'), 'interval': 24}
    try:
        r = requests.get(url, params=params)
        data = r.json()
        candles = data['candles']['data']
        columns = [c.lower() for c in data['candles']['columns']]
        for candle in candles:
            row = dict(zip(columns, candle))
            row['ticker'] = ticker
            moex_data.append(row)
        print(f'{ticker}: {len(candles)}')
    except Exception as e:
        print(f'Ошибка {ticker}: {e}')

if moex_data:
    db.moex_stocks.drop()
    db.moex_stocks.insert_many(moex_data)
    print(f'moex_stocks: {len(moex_data)}')

print('4. Курсы валют...')
currencies = ['USD', 'EUR', 'RUB', 'CNY', 'JPY', 'GBP', 'PLN', 'UAH']
rates = []

for cur in currencies:
    url = f'https://api.nbrb.by/exrates/rates/{cur}?parammode=2'
    try:
        r = requests.get(url, timeout=10)
        data = r.json()
        rates.append({
            'date': datetime.now().strftime('%Y-%m-%d'),
            'currency': cur,
            'rate_to_byn': data.get('Cur_OfficialRate', 0),
            'scale': data.get('Cur_Scale', 1)
        })
        print(f'{cur}: {data.get("Cur_OfficialRate", 0)}')
    except Exception as e:
        print(f'Ошибка {cur}: {e}')

if rates:
    db.currency_rates.drop()
    db.currency_rates.insert_many(rates)
    print(f'currency_rates: {len(rates)}')

print('ГОТОВО!')