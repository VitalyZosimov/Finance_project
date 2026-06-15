from hooks.mongo_hook import MongoHook
import pandas as pd
import plotly.express as px
import os

hook = MongoHook()
db = hook.get_db('stockviz')
os.makedirs('/opt/airflow/output/tickers', exist_ok=True)

# Графики по тикерам
data = list(db.stock_data.find({}, {'_id': 0}))
if data:
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    for ticker in df['ticker'].unique():
        df_t = df[df['ticker'] == ticker].sort_values('date')
        fig = px.line(df_t, x='date', y='close', title=ticker)
        fig.write_html(f'/opt/airflow/output/tickers/{ticker}_price.html')
        print(f'{ticker} OK')

# MOEX
moex = list(db.moex_stocks.find({}, {'_id': 0}))
if moex:
    df = pd.DataFrame(moex)
    df['begin'] = pd.to_datetime(df['begin'])
    for ticker in df['ticker'].unique():
        df_t = df[df['ticker'] == ticker].sort_values('begin')
        fig = px.line(df_t, x='begin', y='close', title=f'{ticker} MOEX')
        fig.write_html(f'/opt/airflow/output/tickers/{ticker}_moex.html')
        print(f'MOEX {ticker} OK')

# Курсы валют
rates = list(db.currency_rates.find({}, {'_id': 0}))
if rates:
    df = pd.DataFrame(rates)
    fig = px.bar(df, x='currency', y='rate_to_byn', title='Currency rates')
    fig.write_html('/opt/airflow/output/currency_rates.html')
    print('Currency rates OK')

# Портфель
port = list(db.portfolio_metrics.find({}, {'_id': 0}))
if port:
    df = pd.DataFrame(port)
    df['date'] = pd.to_datetime(df['date'])
    fig = px.line(df, x='date', y='portfolio_value', title='Portfolio value')
    fig.write_html('/opt/airflow/output/portfolio_value_total.html')
    print('Portfolio OK')

print('Все графики созданы!')