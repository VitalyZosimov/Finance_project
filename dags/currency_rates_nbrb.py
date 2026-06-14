from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from hooks.mongo_hook import MongoHook
import requests

def fetch_nbrb_rates():
    hook = MongoHook()
    db = hook.get_db('stockviz')
    
    currencies = ['USD', 'EUR', 'RUB', 'CNY', 'JPY', 'GBP', 'PLN', 'UAH']
    rates = []
    
    for cur in currencies:
        url = f'https://api.nbrb.by/exrates/rates/{cur}?parammode=2'
        try:
            r = requests.get(url, timeout=10)
            data = r.json()
            rate_val = data.get('Cur_OfficialRate', 0)
            rates.append({
                'date': datetime.now().strftime('%Y-%m-%d'),
                'currency': cur,
                'rate_to_byn': rate_val,
                'scale': data.get('Cur_Scale', 1)
            })
            print(f'{cur}: {rate_val}')
        except Exception as e:
            print(f'Ошибка {cur}: {e}')
    
    if rates:
        db.currency_rates.drop()
        db.currency_rates.insert_many(rates)
        print(f'Загружено {len(rates)} курсов')

with DAG(
    dag_id='currency_rates_nbrb',
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 */6 * * *',
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id='fetch_nbrb_rates',
        python_callable=fetch_nbrb_rates
    )