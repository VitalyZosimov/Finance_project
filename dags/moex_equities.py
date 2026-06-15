from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from hooks.mongo_hook import MongoHook
import requests

def fetch_moex_stocks():
    hook = MongoHook()
    db = hook.get_db('stockviz')
    
    tickers = ['SBER', 'GAZP', 'LKOH', 'ROSN', 'VTBR']
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    all_data = []
    
    for ticker in tickers:
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
                all_data.append(row)
            print(f'{ticker}: {len(candles)} записей')
        except Exception as e:
            print(f'Ошибка {ticker}: {e}')
    
    if all_data:
        db.moex_stocks.drop()
        db.moex_stocks.insert_many(all_data)
        print(f'Сохранено {len(all_data)} записей')

with DAG(
    dag_id='moex_equities',
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 */6 * * *',
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id='fetch_moex_stocks',
        python_callable=fetch_moex_stocks
    )