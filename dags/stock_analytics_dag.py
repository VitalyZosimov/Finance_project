from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.models import Variable

from dags.operators.fetch_stock_operator import FetchStockDataOperator
from dags.operators.calc_metrics_operator import CalculateMetricsOperator
from dags.operators.etl_to_mongo_operator import EtlToMongoOperator
from dags.operators.train_model_operator import TrainModelOperator
from dags.operators.predict_operator import PredictOperator
from dags.operators.generate_charts_operator import GenerateChartsOperator

from dags.hooks.telegram_hook import TelegramAlertHook

#Словарь с параметрами для всех задач в DAG
default_args = {
		'owner': 'data_engineer',               #Владелец DAG
		'depends_on_past': False,               #Не зависеть от успеха предыдущего запуска
		'start_date': datetime(2025, 6, 1),     #Дата первого возможного запуска
		'email': ['amarillisby@gmail.com'],     #Email для уведомлений (заменить для сдачи на e-mail Кирилла)
		'email_on_failure': True,               #Отправлять письмо при ошибке
		'email_on_retry': False,                #Не отправлять письмо при повторе
		'retries': 2,                           #Количество повторных попыток
		'retry_delay': timedelta(minutes=5),    #Задержка между повторами
}

#при успешном завершении всего DAG (отправляет уведомление в Telegram)
def notify_success_telegram(context):
		hook = TelegramAlertHook()              #Создаём экземпляр хука для Telegram (БОТ!!!!)
		dag_id = context['dag'].dag_id          #Получаем ID DAG из контекста
		exec_date = context['execution_date']   #Получаем дату выполнения
		hook.send_message(f"✅ DAG {dag_id} успешно выполнен за {exec_date}")  #Отправляем сообщение

#Создание DAG с именем stock_analytics_pipeline
with DAG(
		dag_id='stock_analytics_pipeline',     						  #Уникальный идентификатор DAG
		default_args=default_args,            						  #Передаём параметры по умолчанию
		description='ETL + нейросеть + витрины для анализа акций',    #Описание
		schedule_interval='0 20 * * *',						          #Запуск каждый день в 20:00 (cron)
		catchup=False,                           					  #Не запускать пропущенные интервалы
		on_success_callback=notify_success_telegram,  				  #Колбэк при успехе всего DAG
) as dag:

		#Задача 1: нужно загрузить данные акций из Yahoo Finance
		fetch_task = FetchStockDataOperator(
				task_id='fetch_stock_data',         				  
				tickers=['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA'], 	  #Список тикеров
				start_date='2020-01-01'           					  #Дата начала исторических данных
		)

		#Задача 2: рассчитать рыночные и портфельные метрики
		calc_task = CalculateMetricsOperator(
				task_id='calculate_metrics',        				  
				weights={'AAPL':0.3, 'MSFT':0.3, 'GOOGL':0.2, 'AMZN':0.1, 'TSLA':0.1},  #Веса активов
				initial_cash=100000                   				  #Начальный капитал портфеля  (можем тут быть очень богатыми)
		)

		#Задача 3: перенос данных из PostgreSQL в MongoDB
		etl_task = EtlToMongoOperator(
				task_id='etl_to_mongo',               				  
				mongo_conn_id='mongo_default'           			  
		)

		#Задача 4: обучить LSTM-модель на данных одного тикера (AAPL)
		train_task = TrainModelOperator(
				task_id='train_model',                
				ticker='AAPL'                         #Какой тикер использовать для обучения (Смотря что выгружаем, доступны еще MSFT, GOOGL, AMZN, TSLA)
		)

		#Задача 5: сделать прогноз волатильности на текущий день
		predict_task = PredictOperator(
				task_id='predict_volatility',         
				ticker='AAPL'                         # Для какого тикера делать прогноз
		)

		#Задача 6: сгенерировать HTML-графики из данных MongoDB
		charts_task = GenerateChartsOperator(
				task_id='generate_charts'            
		)

		#Задача 7: отправить email-отчёт об успешном выполнении
		email_report = EmailOperator(
				task_id='send_email_report',          
				to=Variable.get('alert_email', default_var='amarillisby@gmail.com'),  #Адрес получателя (для сдачи взять e-mail Кирилла)
				subject='Отчёт о выполнении пайплайна акций',  
				html_content='<p>DAG stock_analytics_pipeline выполнен. Графики в output/.</p>'
		)

		#порядок выполнения задач
		fetch_task >> calc_task >> etl_task          #После загрузки считаем метрики, потом ETL
		etl_task >> train_task >> predict_task       #После ETL обучаем модель, затем прогноз
		predict_task >> charts_task >> email_report  #После прогноза строим графики, затем письмо