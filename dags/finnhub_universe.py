from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from hooks.mongo_hook import MongoHook

def load_static_companies():
    companies = [
        {'ticker': 'AAPL', 'name': 'Apple Inc', 'country': 'USA', 'sector': 'Technology', 'currency': 'USD', 'exchange': 'NASDAQ'},
        {'ticker': 'MSFT', 'name': 'Microsoft Corp', 'country': 'USA', 'sector': 'Technology', 'currency': 'USD', 'exchange': 'NASDAQ'},
        {'ticker': 'GOOGL', 'name': 'Alphabet Inc', 'country': 'USA', 'sector': 'Technology', 'currency': 'USD', 'exchange': 'NASDAQ'},
        {'ticker': 'AMZN', 'name': 'Amazon Inc', 'country': 'USA', 'sector': 'Consumer', 'currency': 'USD', 'exchange': 'NASDAQ'},
        {'ticker': 'TSLA', 'name': 'Tesla Inc', 'country': 'USA', 'sector': 'Automotive', 'currency': 'USD', 'exchange': 'NASDAQ'},
        {'ticker': 'META', 'name': 'Meta Platforms', 'country': 'USA', 'sector': 'Technology', 'currency': 'USD', 'exchange': 'NASDAQ'},
        {'ticker': 'NFLX', 'name': 'Netflix Inc', 'country': 'USA', 'sector': 'Entertainment', 'currency': 'USD', 'exchange': 'NASDAQ'},
        {'ticker': 'NVDA', 'name': 'Nvidia Corp', 'country': 'USA', 'sector': 'Technology', 'currency': 'USD', 'exchange': 'NASDAQ'},
        {'ticker': 'AMD', 'name': 'AMD Corp', 'country': 'USA', 'sector': 'Technology', 'currency': 'USD', 'exchange': 'NASDAQ'},
        {'ticker': 'IBM', 'name': 'IBM Corp', 'country': 'USA', 'sector': 'Technology', 'currency': 'USD', 'exchange': 'NYSE'},
        {'ticker': 'NVO', 'name': 'Novo Nordisk', 'country': 'Denmark', 'sector': 'Pharma', 'currency': 'DKK', 'exchange': 'CSE'},
        {'ticker': 'SAP', 'name': 'SAP SE', 'country': 'Germany', 'sector': 'Software', 'currency': 'EUR', 'exchange': 'XETRA'},
        {'ticker': 'TM', 'name': 'Toyota', 'country': 'Japan', 'sector': 'Automotive', 'currency': 'JPY', 'exchange': 'TSE'},
        {'ticker': 'HSBC', 'name': 'HSBC Holdings', 'country': 'UK', 'sector': 'Banking', 'currency': 'GBP', 'exchange': 'LSE'},
    ]
    
    hook = MongoHook()
    db = hook.get_db('stockviz')
    db.companies_universe.drop()
    db.companies_universe.insert_many(companies)
    print(f'Загружено {len(companies)} компаний')
    return 'OK'

with DAG(
    dag_id='finnhub_universe',
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 12 * * 0',
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id='load_static_companies',
        python_callable=load_static_companies
    )