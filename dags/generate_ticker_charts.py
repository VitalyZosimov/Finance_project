from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from hooks.mongo_hook import MongoHook
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

def generate_all_charts():
    hook = MongoHook()
    db = hook.get_db('stockviz')
    
    # Создаём папки
    os.makedirs('/opt/airflow/output', exist_ok=True)
    os.makedirs('/opt/airflow/output/tickers', exist_ok=True)
    
    print('=' * 50)
    print('1. ГРАФИКИ ПО ТЕСТОВЫМ ДАННЫМ (yfinance)')
    print('=' * 50)
    
    data = list(db.stock_data.find({}, {'_id': 0}))
    if data:
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        
        for ticker in df['ticker'].unique():
            df_ticker = df[df['ticker'] == ticker].sort_values('date')
            
            # График цены
            fig1 = px.line(df_ticker, x='date', y='close', title=f'{ticker} — цена закрытия')
            fig1.write_html(f'/opt/airflow/output/tickers/{ticker}_price.html')
            
            # График объёмов
            fig2 = px.bar(df_ticker, x='date', y='volume', title=f'{ticker} — объём торгов')
            fig2.write_html(f'/opt/airflow/output/tickers/{ticker}_volume.html')
            
            print(f'{ticker}: {len(df_ticker)} записей')
    
    print('\n' + '=' * 50)
    print('2. ГРАФИКИ ПО РОССИЙСКИМ АКЦИЯМ (MOEX)')
    print('=' * 50)
    
    moex_data = list(db.moex_stocks.find({}, {'_id': 0}))
    if moex_data:
        df_moex = pd.DataFrame(moex_data)
        df_moex['begin'] = pd.to_datetime(df_moex['begin'])
        
        for ticker in df_moex['ticker'].unique():
            df_ticker = df_moex[df_moex['ticker'] == ticker].sort_values('begin')
            
            # График цены
            fig1 = px.line(df_ticker, x='begin', y='close', title=f'{ticker} (MOEX) — цена закрытия')
            fig1.write_html(f'/opt/airflow/output/tickers/{ticker}_moex_price.html')
            
            # График объёмов
            fig2 = px.bar(df_ticker, x='begin', y='volume', title=f'{ticker} (MOEX) — объём торгов')
            fig2.write_html(f'/opt/airflow/output/tickers/{ticker}_moex_volume.html')
            
            print(f'MOEX {ticker}: {len(df_ticker)} записей')
    
    print('\n' + '=' * 50)
    print('3. КУРСЫ ВАЛЮТ (НБРБ)')
    print('=' * 50)
    
    rates_data = list(db.currency_rates.find({}, {'_id': 0}))
    if rates_data:
        df_rates = pd.DataFrame(rates_data)
        df_rates['date'] = pd.to_datetime(df_rates['date'])
        
        fig = px.bar(df_rates, x='currency', y='rate_to_byn', title='Курсы валют к BYN')
        fig.write_html('/opt/airflow/output/currency_rates.html')
        print(f'currency_rates: {len(df_rates)} записей')
    
    print('\n' + '=' * 50)
    print('4. ПОРТФЕЛЬНЫЕ МЕТРИКИ')
    print('=' * 50)
    
    portfolio_data = list(db.portfolio_metrics.find({}, {'_id': 0}))
    if portfolio_data:
        df_portfolio = pd.DataFrame(portfolio_data)
        df_portfolio['date'] = pd.to_datetime(df_portfolio['date'])
        
        # Стоимость портфеля
        fig1 = px.line(df_portfolio, x='date', y='portfolio_value', title='Стоимость портфеля (общая)')
        fig1.write_html('/opt/airflow/output/portfolio_value_total.html')
        
        # Доходность
        fig2 = px.line(df_portfolio, x='date', y='portfolio_return', title='Доходность портфеля (%)')
        fig2.write_html('/opt/airflow/output/portfolio_return_total.html')
        
        print(f'portfolio_metrics: {len(df_portfolio)} записей')
    
    print('\n' + '=' * 50)
    print('5. ПРОГНОЗЫ LSTM')
    print('=' * 50)
    
    predictions = list(db.predictions.find({}, {'_id': 0}))
    if predictions:
        df_pred = pd.DataFrame(predictions)
        
        fig = px.bar(df_pred, x='ticker', y='predicted_close', title='LSTM прогноз цен закрытия')
        fig.write_html('/opt/airflow/output/lstm_predictions.html')
        print(f'predictions: {len(df_pred)} записей')
    
    print('\n' + '=' * 50)
    print('ВСЕ ГРАФИКИ СОЗДАНЫ!')
    print('Папка: /opt/airflow/output/tickers/')
    print('=' * 50)

with DAG(
    dag_id='generate_ticker_charts',
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 1 * * *',
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id='generate_all_charts',
        python_callable=generate_all_charts
    )