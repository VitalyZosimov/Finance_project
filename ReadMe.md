# Finance Analytics Pipeline — ETL + ML + BI для финансового анализа

[![Python](https://img.shields.io/badge/Python-3.10-blue.svg)](https://www.python.org/)
[![Airflow](https://img.shields.io/badge/Airflow-2.8.1-green.svg)](https://airflow.apache.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-6.0-green.svg)](https://www.mongodb.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue.svg)](https://www.postgresql.org/)
[![Metabase](https://img.shields.io/badge/Metabase-latest-yellow.svg)](https://metabase.com/)
[![TensorFlow](https://img.shields.io/badge/TensorFlow-2.13-orange.svg)](https://tensorflow.org/)
[![Docker](https://img.shields.io/badge/Docker-24.0+-blue.svg)](https://www.docker.com/)

**Production-ready ETL-пайплайн** для финансового анализа: загрузка данных, расчёт портфельных метрик, прогнозирование цен с помощью LSTM, визуализация в Metabase.  
Российские акции (MOEX), курсы валют (НБРБ), Telegram-уведомления — всё в одном проекте.

---

## 📌 Оглавление

- [Архитектура](#архитектура)
- [Технологический стек](#технологический-стек)
- [Быстрый старт](#быстрый-старт)
- [Структура репозитория](#структура-репозитория)
- [Модель данных](#модель-данных)
- [ETL Pipeline](#etl-pipeline)
- [ML Прогнозирование](#ml-прогнозирование)
- [Дашборды](#дашборды)
- [Мониторинг и оповещения](#мониторинг-и-оповещения)
- [Управление проектом](#управление-проектом)

---

## 🏗️ Архитектура
┌────────────────────────────────────────────────────────────────────────────┐
│ Docker Compose                                                             │
├───────────────┬───────────────┬───────────────┬───────────────┬────────────┤
│ Airflow       │ MongoDB       │ PostgreSQL    │ Metabase      │ ML Service │
│ (Web + Sched) │ (Горячие      │ (Холодные     │ (BI/Аналитика)│ (LSTM)     │
│  данные)      │ данные)       │               │               │            │
├───────────────┴───────────────┴───────────────┴───────────────┴────────────┤
│ DAG-файлы                                                                  │
├────────────────────────────────────────────────────────────────────────────┤
│ stock_data_generator.py → Генерация тестовых данных                        │
│ portfolio_calculator.py → Расчёт портфеля + архив + графики + Telegram     │
│ moex_equities.py → Загрузка российских акций (MOEX)                        │
│ currency_rates_nbrb.py → Курсы валют (НБРБ)                                │
│ trigger_ml_predict.py → Запуск LSTM прогноза после ETL                     │
└────────────────────────────────────────────────────────────────────────────┘


---

## 🛠️ Технологический стек

| Компонент        | Технология                | Версия      |
|------------------|---------------------------|-------------|
| **Orchestration**| Apache Airflow            | 2.8.1       |
| **Raw Storage**  | MongoDB                   | 6.0         |
| **DDS (Archive)**| PostgreSQL                | 15          |
| **BI**           | Metabase                  | latest      |
| **ML**           | TensorFlow / Keras / LSTM | 2.13        |
| **API Clients**  | requests, yfinance, moex-wrapper | —       |
| **Visualization**| Plotly, HTML charts       | 5.17        |
| **Notifications**| Telegram Bot API          | —           |
| **Container**    | Docker + Docker Compose   | 24.0+       |

---

## 🚀 Быстрый старт

### 1. Клонирование репозитория

git clone https://github.com/your-username/finance_project.git
cd finance_project

### 2. Запуск всех сервисов
docker compose up -d

### 3. Инициализация данных (один раз)
docker exec -it airflow_webserver airflow connections add "mongo_default" --conn-type mongodb --conn-host fin_mongo --conn-port 27017 --conn-login mongo --conn-password mongo
docker cp get_all_data.py airflow_webserver:/tmp/
docker exec -it airflow_webserver python /tmp/get_all_data.py

Скрипт get_all_data.py автоматически:

генерирует тестовые данные (10 960 записей)
рассчитывает портфель (2 192 записи)
загружает российские акции с MOEX (155 записей)
загружает курсы валют НБРБ (8 записей)

Актуальность данных на 15/06/2026

### 4. Доступ к сервисам

Сервис	        URL	                    Логин	            Пароль
Airflow	        http://localhost:8080	admin	            admin
Metabase    	http://localhost:3000	(регистрация)   	—
MongoDB     	localhost:27017     	mongo	            mongo
PostgreSQL	    localhost:5432 / 5433	postgres / airflow	postgres / airflow

📁 Структура репозитория
finance_project/
├── dags/
│   ├── hooks/
│   │   ├── mongo_hook.py
│   │   └── telegram_hook.py
│   ├── operators/
│   │   ├── fetch_stock_operator.py
│   │   ├── calc_metrics_operator.py
│   │   ├── etl_to_mongo_operator.py
│   │   └── generate_charts_operator.py
│   ├── stock_data_generator.py
│   ├── portfolio_calculator.py
│   ├── moex_equities.py
│   ├── currency_rates_nbrb.py
│   └── trigger_ml_predict.py
├── ml_service/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── train.py
│   └── predict.py
├── output/                     # HTML-графики
├── docker-compose.yml
├── docker-compose.ml.yml
├── get_all_data.py
├── requirements.txt
└── README.md

📊 Модель данных

MongoDB — горячее хранилище (stockviz)

Коллекция       	Количество	        Описание
stock_data	        10 960              Цены акций (AAPL, MSFT, GOOGL, AMZN, TSLA) за 2020-2025
portfolio_metrics	2 192	            Ежедневная стоимость и доходность портфеля
moex_stocks     	155             	Российские акции (SBER, GAZP, LKOH, ROSN, VTBR)
currency_rates  	8	                Курсы валют к BYN (НБРБ)
predictions     	5	                Прогнозы LSTM

PostgreSQL — холодное хранилище (finance)

Таблица         	Описание
portfolio_archive	Архив портфеля с created_at (TTL 3 дня)

🔄 ETL Pipeline

DAG-файлы и расписание

DAG	                    Расписание	    Назначение
stock_data_generator	0 */6 * * *	    Генерация тестовых данных
portfolio_calculator	30 */6 * * *	Расчёт портфеля, сохранение в MongoDB/PostgreSQL, графики, Telegram
moex_equities       	0 */6 * * *	    Загрузка российских акций с MOEX
currency_rates_nbrb 	0 */6 * * *	    Загрузка курсов валют НБРБ
trigger_ml_predict  	45 */6 * * *	Запуск LSTM прогноза после обновления данных

Запуск ETL вручную
# Генерация всех данных одной командой
docker cp get_all_data.py airflow_webserver:/tmp/
docker exec -it airflow_webserver python /tmp/get_all_data.py

# Или по отдельности:
docker exec -it airflow_webserver airflow dags trigger stock_data_generator
docker exec -it airflow_webserver airflow dags trigger portfolio_calculator
docker exec -it airflow_webserver airflow dags trigger moex_equities
docker exec -it airflow_webserver airflow dags trigger currency_rates_nbrb

🧠 ML Прогнозирование

Модель: LSTM (2 слоя по 50 нейронов, Dropout 20%)
Признаки: close price (нормализованная)
Окно: 60 дней
Прогноз: цена закрытия на следующий день

Обучение и прогноз
# Запуск ML-контейнера
docker compose -f docker-compose.ml.yml up -d

# Обучение модели для всех тикеров
docker exec -it ml_service python /app/train.py

# Прогноз
docker exec -it ml_service python /app/predict.py

Результаты сохраняются в MongoDB (коллекция predictions) и отображаются в Metabase.

📈 Дашборды

Metabase
Подключение к MongoDB:

Host: fin_mongo
Port: 27017
Database: stockviz
User: mongo
Password: mongo
Доступные коллекции для анализа:
portfolio_metrics → динамика портфеля
stock_data → цены акций
moex_stocks → российские акции
currency_rates → курсы валют
predictions → прогнозы LSTM

HTML-графики (автоматическая генерация)

После выполнения portfolio_calculator в папке output/ создаются:
portfolio_value.html
portfolio_return.html
portfolio_histogram.html
drawdown.html

📢 Мониторинг и оповещения
При успешном или ошибочном завершении DAG portfolio_calculator отправляется сообщение в Telegram.
Переменные Airflow:
bot_token — токен Telegram бота
tg_chat_id — ID чата

Проверка:
docker exec -it airflow_webserver python -c "
from hooks.telegram_hook import TelegramAlertHook
TelegramAlertHook().send_message('Тест из Airflow')
"

Healthchecks (Docker)
fin_mongo — mongosh --eval "db.adminCommand('ping')"
airflow_postgres / fin_postgres — pg_isready
airflow_webserver — curl http://localhost:8080/health

🧹 Управление проектом

# Полный сброс и перезапуск
docker compose down -v
docker compose up -d

# Просмотр логов
docker logs airflow_webserver --tail 50
docker logs airflow_scheduler --tail 50
docker logs fin_mongo --tail 30

Очистка старых данных (PostgreSQL)
Таблица portfolio_archive автоматически удаляет записи старше 3 дней (TTL через created_at).


📄 Лицензия
MIT

👤 Автор
Виталий Зосимов
