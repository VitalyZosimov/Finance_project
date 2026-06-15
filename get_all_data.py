from hooks.mongo_hook import MongoHook
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests


# 1. ПОДКЛЮЧЕНИЕ К MONGODB

hook = MongoHook()
db = hook.get_db('stockviz')


# 2. ГЕНЕРАЦИЯ ТЕСТОВЫХ ДАННЫХ

print('1. Генерация тестовых данных...')
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
print(f'   stock_data: {len(all_data)} записей')


# 3. РАСЧЁТ ПОРТФЕЛЯ

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

portfolio_value = 100000 * (1 + portfolio_returns).cumprod()
result = pd.DataFrame({
    'date': portfolio_returns.index,
    'portfolio_return': portfolio_returns,
    'portfolio_value': portfolio_value
})

db.portfolio_metrics.drop()
db.portfolio_metrics.insert_many(result.to_dict('records'))
print(f'   portfolio_metrics: {len(result)} записей')


# 4. ЗАГРУЗКА MOEX

print('3. Загрузка российских акций (MOEX)...')
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
        print(f'   {ticker}: {len(candles)} записей')
    except Exception as e:
        print(f'   Ошибка {ticker}: {e}')

if moex_data:
    db.moex_stocks.drop()
    db.moex_stocks.insert_many(moex_data)
    print(f'   moex_stocks: {len(moex_data)} записей')


# 5. ЗАГРУЗКА КУРСОВ ВАЛЮТ

print('4. Загрузка курсов валют (НБРБ)...')
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
        print(f'   {cur}: {data.get("Cur_OfficialRate", 0)}')
    except Exception as e:
        print(f'   Ошибка {cur}: {e}')

if rates:
    db.currency_rates.drop()
    db.currency_rates.insert_many(rates)
    print(f'   currency_rates: {len(rates)} записей')


# 6. ГЕНЕРАЦИЯ HTML-ГРАФИКОВ

print('5. Генерация HTML-графиков...')
import plotly.express as px
import os

os.makedirs('/opt/airflow/output/tickers', exist_ok=True)

# Графики по тестовым данным
data = list(db.stock_data.find({}, {'_id': 0}))
if data:
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    for ticker in df['ticker'].unique():
        df_t = df[df['ticker'] == ticker].sort_values('date')
        fig = px.line(df_t, x='date', y='close', title=f'{ticker} цена')
        fig.write_html(f'/opt/airflow/output/tickers/{ticker}_price.html')
    print('   Тестовые графики созданы')

# Графики по MOEX
moex = list(db.moex_stocks.find({}, {'_id': 0}))
if moex:
    df = pd.DataFrame(moex)
    df['begin'] = pd.to_datetime(df['begin'])
    for ticker in df['ticker'].unique():
        df_t = df[df['ticker'] == ticker].sort_values('begin')
        fig = px.line(df_t, x='begin', y='close', title=f'{ticker} MOEX')
        fig.write_html(f'/opt/airflow/output/tickers/{ticker}_moex.html')
    print('   MOEX графики созданы')

# Курсы валют
rates = list(db.currency_rates.find({}, {'_id': 0}))
if rates:
    df = pd.DataFrame(rates)
    fig = px.bar(df, x='currency', y='rate_to_byn', title='Курсы валют к BYN')
    fig.write_html('/opt/airflow/output/currency_rates.html')
    print('   График курсов валют создан')

# Портфель
port = list(db.portfolio_metrics.find({}, {'_id': 0}))
if port:
    df = pd.DataFrame(port)
    df['date'] = pd.to_datetime(df['date'])
    fig = px.line(df, x='date', y='portfolio_value', title='Стоимость портфеля')
    fig.write_html('/opt/airflow/output/portfolio_value_total.html')
    print('   График портфеля создан')

print('   Все графики сохранены в /opt/airflow/output/tickers/')

print('\n ВСЕ ДАННЫЕ И ГРАФИКИ УСПЕШНО ЗАГРУЖЕНЫ!')