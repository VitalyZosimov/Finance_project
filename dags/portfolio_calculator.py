from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from hooks.mongo_hook import MongoHook
from hooks.telegram_hook import TelegramAlertHook
import pandas as pd
import numpy as np
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
import os

def save_to_postgresql(df):
    conn = psycopg2.connect(
        host='fin_postgres',
        port=5432,
        database='finance',
        user='postgres',
        password='postgres'
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS portfolio_archive (
            id SERIAL PRIMARY KEY,
            date DATE UNIQUE NOT NULL,
            portfolio_value NUMERIC(20,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO portfolio_archive (date, portfolio_value, created_at)
            VALUES (%s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (date) DO UPDATE SET portfolio_value = EXCLUDED.portfolio_value
        """, (row['date'], row['portfolio_value']))
    conn.commit()
    cur.close()
    conn.close()
    print(f'Сохранено {len(df)} записей в PostgreSQL')

def calculate_portfolio_with_validation():
    hook = MongoHook()
    
    # Валидируем данные перед расчётом
    try:
        stock_data = hook.validate_and_get_stock_data()
        print(f'Данные прошли валидацию: {stock_data.total_count} записей')
        print(f'Тикеры: {stock_data.tickers_list}')
    except Exception as e:
        print(f'Ошибка валидации: {e}')
        raise

def generate_charts(df):
    os.makedirs('/opt/airflow/output', exist_ok=True)
    
    fig1 = px.line(df, x='date', y='portfolio_value', title='Стоимость портфеля')
    fig1.write_html('/opt/airflow/output/portfolio_value.html')
    
    fig2 = px.line(df, x='date', y='portfolio_return', title='Доходность портфеля')
    fig2.write_html('/opt/airflow/output/portfolio_return.html')
    
    fig3 = px.histogram(df, x='portfolio_return', nbins=50, title='Распределение доходности')
    fig3.write_html('/opt/airflow/output/portfolio_histogram.html')
    
    df['cummax'] = df['portfolio_value'].cummax()
    df['drawdown'] = (df['portfolio_value'] - df['cummax']) / df['cummax'] * 100
    fig4 = go.Figure()
    fig4.add_trace(go.Scatter(x=df['date'], y=df['drawdown'], fill='tozeroy', name='Просадка, %'))
    fig4.update_layout(title='Просадка портфеля')
    fig4.write_html('/opt/airflow/output/drawdown.html')
    
    print('Графики сохранены')

def calculate_portfolio():
    hook = MongoHook()
    db = hook.get_db('stockviz')
    data = list(db.stock_data.find({}, {'_id': 0}))
    
    if not data:
        raise ValueError('Нет данных! Сначала запустите stock_data_generator')
    
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
    save_to_postgresql(result)
    generate_charts(result)
    
    print(f'Рассчитано {len(result)} записей портфеля')
    return 'OK'

def send_success_notification(context):
    hook = TelegramAlertHook()
    dag_id = context['dag'].dag_id
    exec_date = context['execution_date']
    hook.send_message(f'✅ DAG {dag_id} успешно выполнен за {exec_date}')

def send_failure_notification(context):
    hook = TelegramAlertHook()
    dag_id = context['dag'].dag_id
    exec_date = context['execution_date']
    error = context.get('exception', 'Неизвестная ошибка')
    hook.send_message(f'❌ DAG {dag_id} завершился с ошибкой за {exec_date}\nОшибка: {str(error)[:200]}')

with DAG(
    dag_id='portfolio_calculator',
    start_date=datetime(2024, 1, 1),
    schedule_interval='30 */6 * * *',
    catchup=False,
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
) as dag:
    task = PythonOperator(
        task_id='calculate_portfolio',
        python_callable=calculate_portfolio
    )