from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from hooks.mongo_hook import MongoHook
import requests

def fetch_portfolio_countries():
    API_KEY = 'd8ktcshr01qut1f7p56gd8ktcshr01qut1f7p570'
    tickers_str = Variable.get('TICKERS', default_var='AAPL,MSFT,GOOGL,AMZN,TSLA')
    tickers = [t.strip() for t in tickers_str.split(',')]
    
    companies = []
    for ticker in tickers:
        url = f'https://finnhub.io/api/v1/stock/profile2?symbol={ticker}&token={API_KEY}'
        try:
            response = requests.get(url)
            data = response.json()
            companies.append({
                'ticker': ticker,
                'name': data.get('name', ticker),
                'country': data.get('country', 'USA'),
                'currency': data.get('currency', 'USD')
            })
            print(f'{ticker}: {data.get("country", "USA")}')
        except Exception as e:
            print(f'Ошибка {ticker}: {e}')
    
    hook = MongoHook()
    db = hook.get_db('stockviz')
    db.portfolio_companies.drop()
    if companies:
        db.portfolio_companies.insert_many(companies)
    
    print(f'Загружено {len(companies)} компаний портфеля')
    return 'OK'

with DAG(
    dag_id='company_countries',
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 12 * * 0',
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id='fetch_portfolio_countries',
        python_callable=fetch_portfolio_countries
    )