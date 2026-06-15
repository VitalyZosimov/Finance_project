from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from hooks.mongo_hook import MongoHook
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

def generate_charts():
    hook = MongoHook()
    db = hook.get_db('stockviz')
    
    # Загружаем портфельные метрики
    portfolio = pd.DataFrame(list(db.portfolio_metrics.find({}, {'_id': 0})))
    
    if portfolio.empty:
        # Если нет портфельных метрик, создаём тестовые
        print('Нет данных portfolio_metrics, создаём тестовые...')
        dates = pd.date_range(start='2024-01-01', end='2024-12-31', freq='D')
        portfolio = pd.DataFrame({
            'date': dates,
            'portfolio_value': 100000 * (1 + 0.0005 * np.arange(len(dates)) + np.random.randn(len(dates)) * 500),
            'portfolio_return': np.random.randn(len(dates)) * 0.01
        })
    
    portfolio['date'] = pd.to_datetime(portfolio['date'])
    portfolio = portfolio.sort_values('date')
    
    # Создаём папку output
    os.makedirs('/opt/airflow/output', exist_ok=True)
    
    # График 1: стоимость портфеля
    fig1 = px.line(portfolio, x='date', y='portfolio_value', title='Стоимость портфеля')
    fig1.write_html('/opt/airflow/output/portfolio_value.html')
    
    # График 2: доходность
    if 'portfolio_return' in portfolio.columns:
        fig2 = px.line(portfolio, x='date', y='portfolio_return', title='Доходность портфеля')
        fig2.write_html('/opt/airflow/output/portfolio_return.html')
    
    # График 3: гистограмма доходности
    if 'portfolio_return' in portfolio.columns:
        fig3 = px.histogram(portfolio, x='portfolio_return', nbins=50, title='Распределение доходности')
        fig3.write_html('/opt/airflow/output/portfolio_histogram.html')
    
    # График 4: просадка
    portfolio['cummax'] = portfolio['portfolio_value'].cummax()
    portfolio['drawdown'] = (portfolio['portfolio_value'] - portfolio['cummax']) / portfolio['cummax'] * 100
    fig4 = go.Figure()
    fig4.add_trace(go.Scatter(x=portfolio['date'], y=portfolio['drawdown'], fill='tozeroy', name='Просадка, %'))
    fig4.update_layout(title='Просадка портфеля')
    fig4.write_html('/opt/airflow/output/drawdown.html')
    
    print('Графики сохранены в /opt/airflow/output/')
    return 'OK'

with DAG(
    dag_id='generate_portfolio_charts',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id='generate_charts',
        python_callable=generate_charts
    )