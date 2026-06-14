from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from hooks.telegram_hook import TelegramAlertHook
import subprocess

def run_ml_predict():
    try:
        result = subprocess.run(
            ['docker', 'exec', 'ml_service', 'python', '/app/predict.py'],
            capture_output=True,
            text=True,
            timeout=300
        )
        print(result.stdout)
        if result.returncode != 0:
            raise Exception(result.stderr)
        return 'ML прогноз выполнен'
    except Exception as e:
        print(f'Ошибка ML: {e}')
        raise

def send_ml_success(context):
    hook = TelegramAlertHook()
    hook.send_message('🤖 ML прогноз успешно выполнен!')

def send_ml_failure(context):
    hook = TelegramAlertHook()
    hook.send_message(f'❌ Ошибка ML прогноза')

with DAG(
    dag_id='trigger_ml_predict',
    start_date=datetime(2024, 1, 1),
    schedule_interval='45 */6 * * *',
    catchup=False,
    on_success_callback=send_ml_success,
    on_failure_callback=send_ml_failure,
) as dag:
    
    wait_portfolio = ExternalTaskSensor(
        task_id='wait_portfolio_calculator',
        external_dag_id='portfolio_calculator',
        external_task_id='calculate_portfolio',
        timeout=3600,
        poke_interval=30,
        mode='poke'
    )
    
    wait_moex = ExternalTaskSensor(
        task_id='wait_moex_equities',
        external_dag_id='moex_equities',
        external_task_id='fetch_moex_stocks',
        timeout=3600,
        poke_interval=30,
        mode='poke'
    )
    
    wait_currency = ExternalTaskSensor(
        task_id='wait_currency_rates',
        external_dag_id='currency_rates_nbrb',
        external_task_id='fetch_nbrb_rates',
        timeout=3600,
        poke_interval=30,
        mode='poke'
    )
    
    ml_predict = PythonOperator(
        task_id='ml_predict',
        python_callable=run_ml_predict
    )
    
    [wait_portfolio, wait_moex, wait_currency] >> ml_predict