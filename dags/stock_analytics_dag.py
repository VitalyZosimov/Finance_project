# Импорт модуля datetime для работы с датами
from datetime import datetime, timedelta
# Импорт классов Airflow: DAG (основной контейнер), EmailOperator для писем, Variable для переменных
from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.models import Variable

# Импорт наших кастомных операторов из папки operators
from dags.operators.fetch_stock_operator import FetchStockDataOperator
from dags.operators.calc_metrics_operator import CalculateMetricsOperator
from dags.operators.etl_to_mongo_operator import EtlToMongoOperator
from dags.operators.train_model_operator import TrainModelOperator
from dags.operators.predict_operator import PredictOperator
from dags.operators.generate_charts_operator import GenerateChartsOperator
# Импорт хука для Telegram
from dags.hooks.telegram_hook import TelegramAlertHook

# Словарь с параметрами по умолчанию для всех задач в DAG
default_args = {
		'owner': 'data_engineer',               # Владелец DAG
		'depends_on_past': False,               # Не зависеть от успеха предыдущего запуска
		'start_date': datetime(2025, 6, 1),     # Дата первого возможного запуска
		'email': ['admin@example.com'],         # Email для уведомлений
		'email_on_failure': True,               # Отправлять письмо при ошибке
		'email_on_retry': False,                # Не отправлять письмо при повторе
		'retries': 2,                           # Количество повторных попыток
		'retry_delay': timedelta(minutes=5),    # Задержка между повторами
}

# Функция, вызываемая при успешном завершении всего DAG (отправляет уведомление в Telegram)
def notify_success_telegram(context):
		hook = TelegramAlertHook()              # Создаём экземпляр хука для Telegram
		dag_id = context['dag'].dag_id          # Получаем ID DAG из контекста
		exec_date = context['execution_date']   # Получаем дату выполнения
		hook.send_message(f"✅ DAG {dag_id} успешно выполнен за {exec_date}")  # Отправляем сообщение

# Создание DAG с именем stock_analytics_pipeline
with DAG(
		dag_id='stock_analytics_pipeline',       # Уникальный идентификатор DAG
		default_args=default_args,               # Передаём параметры по умолчанию
		description='ETL + нейросеть + витрины для анализа акций',  # Описание
		schedule_interval='0 20 * * *',          # Запуск каждый день в 20:00 (cron)
		catchup=False,                           # Не запускать пропущенные интервалы
		on_success_callback=notify_success_telegram,  # Колбэк при успехе всего DAG
) as dag:

		# Задача 1: загрузить данные акций из Yahoo Finance
		fetch_task = FetchStockDataOperator(
				task_id='fetch_stock_data',           # Уникальное имя задачи
				tickers=['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA'],  # Список тикеров
				start_date='2020-01-01'               # Дата начала исторических данных
		)

		# Задача 2: рассчитать рыночные и портфельные метрики
		calc_task = CalculateMetricsOperator(
				task_id='calculate_metrics',          # Имя задачи
				weights={'AAPL':0.3, 'MSFT':0.3, 'GOOGL':0.2, 'AMZN':0.1, 'TSLA':0.1},  # Веса активов
				initial_cash=100000                   # Начальный капитал портфеля
		)

		# Задача 3: перенести данные из PostgreSQL в MongoDB (витрины)
		etl_task = EtlToMongoOperator(
				task_id='etl_to_mongo',               # Имя задачи
				mongo_conn_id='mongo_default'         # ID подключения к MongoDB (хранится в Airflow)
		)

		# Задача 4: обучить LSTM-модель на данных одного тикера (AAPL)
		train_task = TrainModelOperator(
				task_id='train_model',                # Имя задачи
				ticker='AAPL'                         # Какой тикер использовать для обучения
		)

		# Задача 5: сделать прогноз волатильности на текущий день
		predict_task = PredictOperator(
				task_id='predict_volatility',         # Имя задачи
				ticker='AAPL'                         # Для какого тикера делать прогноз
		)

		# Задача 6: сгенерировать HTML-графики из данных MongoDB
		charts_task = GenerateChartsOperator(
				task_id='generate_charts'             # Имя задачи
		)

		# Задача 7: отправить email-отчёт об успешном выполнении
		email_report = EmailOperator(
				task_id='send_email_report',          # Имя задачи
				to=Variable.get('alert_email', default_var='admin@example.com'),  # Адрес получателя
				subject='Отчёт о выполнении пайплайна акций',  # Тема письма
				html_content='<p>DAG stock_analytics_pipeline выполнен. Графики в output/.</p>'  # Тело письма
		)

		# Определяем порядок выполнения задач (граф зависимостей)
		fetch_task >> calc_task >> etl_task       # После загрузки считаем метрики, потом ETL
		etl_task >> train_task >> predict_task    # После ETL обучаем модель, затем прогноз
		predict_task >> charts_task >> email_report  # После прогноза строим графики, затем письмо