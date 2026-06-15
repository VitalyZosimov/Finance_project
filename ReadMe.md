# 📈 Finance Analytics Pipeline — ETL + ML + BI для финансового анализа

[![Python](https://img.shields.io/badge/Python-3.10-blue.svg)](https://www.python.org/)
[![Airflow](https://img.shields.io/badge/Airflow-2.8.1-green.svg)](https://airflow.apache.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-6.0-green.svg)](https://www.mongodb.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue.svg)](https://www.postgresql.org/)
[![Metabase](https://img.shields.io/badge/Metabase-latest-yellow.svg)](https://metabase.com/)
[![TensorFlow](https://img.shields.io/badge/TensorFlow-2.13-orange.svg)](https://tensorflow.org/)
[![Docker](https://img.shields.io/badge/Docker-24.0+-blue.svg)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

**Production-ready ETL/ELT пайплайн** для финансового анализа с автоматической загрузкой данных, расчётом портфельных метрик, прогнозированием цен с помощью LSTM и визуализацией в Metabase. Проект полностью контейнеризирован (Docker) и готов к развёртыванию в любой среде.

---

## 📌 Оглавление

- [О проекте](#о-проекте)
- [Архитектура](#архитектура)
- [Технологический стек](#технологический-стек)
- [Быстрый старт](#быстрый-старт)
- [Структура репозитория](#структура-репозитория)
- [Модель данных](#модель-данных)
- [ETL Pipeline (DAG)](#etl-pipeline-dag)
- [Машинное обучение (LSTM)](#машинное-обучение-lstm)
- [Визуализация](#визуализация)
- [Уведомления (Telegram)](#уведомления-telegram)
- [API источники данных](#api-источники-данных)
- [Управление проектом](#управление-проектом)
- [Планы по развитию](#планы-по-развитию)
- [Лицензия](#лицензия)

---

## 🎯 О проекте

Проект решает задачу автоматизации сбора, трансформации и анализа финансовых данных. Основные возможности:

- 🔄 **Автоматический ETL**: загрузка данных из внешних API (MOEX, НБРБ) и генерация тестовых данных
- 📊 **Расчёт портфеля**: ежедневная стоимость, доходность, просадки, VaR
- 🧠 **ML прогнозирование**: LSTM модель для предсказания цен акций
- 📈 **Визуализация**: интерактивные HTML-графики (Plotly) и BI-дашборды (Metabase)
- 🤖 **Уведомления**: Telegram-оповещения о статусе выполнения
- 🐳 **Контейнеризация**: все сервисы запускаются через Docker Compose

---

## 🏗️ Архитектура
┌─────────────────────────────────────────────────────────────────────────────────┐
│ DOCKER │
├───────────────┬───────────────┬───────────────┬───────────────┬─────────────────┤
│ Airflow │ MongoDB │ PostgreSQL │ Metabase │ ML Service │
│ (оркестрация)│ (горячие │ (холодные │ (BI/даш- │ (LSTM прогноз) │
│ │ данные) │ данные) │ борды) │ │
├───────────────┴───────────────┴───────────────┴───────────────┴─────────────────┤
│ DAG-файлы (пайплайны) │
├─────────────────────────────────────────────────────────────────────────────────┤
│ stock_data_generator.py → генерация тестовых данных (10 960 записей) │
│ portfolio_calculator.py → расчёт портфеля + архив + графики + Telegram │
│ moex_equities.py → загрузка российских акций (MOEX, 155 записей) │
│ currency_rates_nbrb.py → загрузка курсов валют (НБРБ, 8 записей) │
│ trigger_ml_predict.py → запуск LSTM прогноза после ETL (ExternalTaskSensor) │
└─────────────────────────────────────────────────────────────────────────────────┘

text

---

## 🛠️ Технологический стек

| Компонент | Технология | Версия | Назначение |
|-----------|------------|--------|------------|
| **Оркестрация** | Apache Airflow | 2.8.1 | Управление ETL-пайплайнами |
| **Горячее хранилище** | MongoDB | 6.0 | Хранение сырых и обработанных данных |
| **Холодное хранилище** | PostgreSQL | 15 | Архив портфеля (TTL 3 дня) |
| **BI/Аналитика** | Metabase | latest | Дашборды и визуализация |
| **Машинное обучение** | TensorFlow / Keras | 2.13 | LSTM для прогноза цен |
| **Контейнеризация** | Docker / Docker Compose | 24.0+ | Изоляция и запуск сервисов |
| **API клиенты** | requests, moex-wrapper | — | Загрузка данных из внешних API |
| **Визуализация** | Plotly | 5.17 | Интерактивные HTML-графики |
| **Уведомления** | Telegram Bot API | — | Оповещения о статусе DAG |

---

## 🚀 Быстрый старт

### Предварительные требования

- Docker Desktop 24.0+
- 8+ GB RAM
- 10+ GB свободного места

### Установка и запуск


# 1. Клонирование репозитория
git clone https://github.com/VitalyZosimov/Finance_project.git
cd Finance_project

# 2. Запуск всех сервисов
docker compose up -d

# 3. Ожидание инициализации (30 секунд)
sleep 30

# 4. Настройка подключения к MongoDB
docker exec -it airflow_webserver airflow connections add "mongo_default" --conn-type mongodb --conn-host fin_mongo --conn-port 27017 --conn-login mongo --conn-password mongo

# 5. Загрузка всех данных (генерация + MOEX + курсы валют)
docker cp get_all_data.py airflow_webserver:/tmp/
docker exec -it airflow_webserver python /tmp/get_all_data.py

# 6. Запуск ML-сервиса и прогноза
docker compose -f docker-compose.ml.yml up -d
docker exec -it ml_service python /app/train.py
docker exec -it ml_service python /app/predict.py

# 7. Генерация HTML-графиков
docker cp charts.py airflow_webserver:/tmp/
docker exec -it airflow_webserver python /tmp/charts.py
Доступ к сервисам
Сервис	URL	Логин	Пароль
Airflow UI	http://localhost:8080	admin	admin
Metabase	http://localhost:3000	(регистрация)	—
MongoDB	localhost:27017	mongo	mongo
PostgreSQL (finance)	localhost:5432	postgres	postgres
PostgreSQL (airflow)	localhost:5433	airflow	airflow

## 📁 Структура репозитория

Finance_project/
├── dags/ # DAG-файлы Airflow
│ ├── hooks/ # Кастомные хуки
│ │ ├── mongo_hook.py # Подключение к MongoDB
│ │ └── telegram_hook.py # Отправка сообщений в Telegram
│ ├── operators/ # Кастомные операторы
│ │ ├── portfolio_calculator.py # Расчёт портфеля
│ │ ├── stock_data_generator.py # Генерация тестовых данных
│ │ ├── moex_equities.py # Загрузка MOEX (Россия)
│ │ ├── currency_rates_nbrb.py # Курсы валют НБРБ (Беларусь)
│ │ └── trigger_ml_predict.py # Запуск ML прогноза
│ └── ...
│
├── ml_service/ # Отдельный ML-контейнер
│ ├── models/ # Сохранённые модели
│ │ ├── lstm_aapl.h5 # Обученная LSTM модель
│ │ └── scaler_aapl.pkl # Нормализатор данных
│ ├── train.py # Обучение LSTM модели
│ ├── predict.py # Прогнозирование
│ ├── requirements.txt # Python зависимости
│ └── Dockerfile # Сборка ML-образа
│
├── output/ # Генерируемые графики
│ ├── tickers/ # Графики по тикерам
│ │ ├── AAPL_price.html
│ │ ├── SBER_moex.html
│ │ └── ...
│ ├── currency_rates.html # Курсы валют
│ └── portfolio_value_total.html # Стоимость портфеля
│
├── docker-compose.yml # Основные сервисы
├── docker-compose.ml.yml # ML-сервис (TensorFlow)
├── get_all_data.py # Единый скрипт загрузки данных
├── charts.py # Генерация HTML-графиков
├── requirements.txt # Python зависимости
├── .env # Переменные окружения
└── README.md # Документация

**Описание ключевых директорий:**

- **`dags/`** — содержит DAG-файлы Airflow, хуки для подключения к БД и операторы для ETL
- **`ml_service/`** — изолированный контейнер с TensorFlow для обучения LSTM модели
- **`output/`** — автоматически генерируемые HTML-графики (Plotly) для визуализации
  
📊 Модель данных

MongoDB — горячее хранилище (stockviz)
Коллекция	Количество записей	Описание
stock_data	10 960	Тестовые данные (AAPL, MSFT, GOOGL, AMZN, TSLA) за 2020-2025
portfolio_metrics	2 192	Ежедневная стоимость и доходность портфеля
moex_stocks	155	Российские акции (SBER, GAZP, LKOH, ROSN, VTBR)
currency_rates	8	Курсы валют к BYN (USD, EUR, RUB, CNY, JPY, GBP, PLN, UAH)
predictions	5	LSTM прогнозы цен закрытия
PostgreSQL — холодное хранилище (finance)
Таблица	Описание	TTL
portfolio_archive	Архив портфеля с датой создания (created_at)	3 дня (автоочистка)

🔄 ETL Pipeline (DAG)

Расписание
DAG	Расписание	Назначение
stock_data_generator	0 */6 * * *	Генерация тестовых данных
portfolio_calculator	30 */6 * * *	Расчёт портфеля + архив + графики + Telegram
moex_equities	0 */6 * * *	Загрузка российских акций (MOEX)
currency_rates_nbrb	0 */6 * * *	Загрузка курсов валют НБРБ
trigger_ml_predict	45 */6 * * *	Запуск LSTM прогноза после всех ETL

Запуск вручную
bash
# Генерация всех данных одной командой
docker cp get_all_data.py airflow_webserver:/tmp/
docker exec -it airflow_webserver python /tmp/get_all_data.py

# Или по отдельности:
docker exec -it airflow_webserver airflow dags trigger stock_data_generator
docker exec -it airflow_webserver airflow dags trigger portfolio_calculator
docker exec -it airflow_webserver airflow dags trigger moex_equities
docker exec -it airflow_webserver airflow dags trigger currency_rates_nbrb

🧠 Машинное обучение (LSTM)

Архитектура модели

text
Input (60 дней)
    ↓
LSTM(50, return_sequences=True)
    ↓
Dropout(0.2)
    ↓
LSTM(50, return_sequences=False)
    ↓
Dropout(0.2)
    ↓
Dense(25, activation='relu')
    ↓
Dense(1) → прогноз цены
Параметры обучения
Параметр	Значение
Sequence length	60 дней
Train/Test split	80/20
Epochs	20
Batch size	32
Optimizer	Adam
Loss function	MSE
Metric	MAE
Запуск обучения и прогноза

bash
# Запуск ML-контейнера
docker compose -f docker-compose.ml.yml up -d

# Обучение модели
docker exec -it ml_service python /app/train.py

# Прогнозирование
docker exec -it ml_service python /app/predict.py
Результаты сохраняются в MongoDB (коллекция predictions).

📊 Визуализация

Metabase
Подключение к MongoDB:

Host: fin_mongo

Port: 27017

Database name: stockviz

Username: mongo

Password: mongo

Authentication database: admin

После синхронизации доступны коллекции:

portfolio_metrics — динамика портфеля

stock_data — цены акций

moex_stocks — российские акции

currency_rates — курсы валют

predictions — прогнозы LSTM

HTML-графики (Plotly)
Автоматически генерируются в папке output/tickers/:

Тип	Файлы

Тестовые данные (yfinance)	AAPL_price.html, MSFT_price.html, GOOGL_price.html, AMZN_price.html, TSLA_price.html
MOEX (Россия)	SBER_moex.html, GAZP_moex.html, LKOH_moex.html, ROSN_moex.html, VTBR_moex.html
Курсы валют	currency_rates.html
Портфель	portfolio_value_total.html

bash

# Генерация графиков
docker cp charts.py airflow_webserver:/tmp/
docker exec -it airflow_webserver python /tmp/charts.py

# Копирование на хост
docker cp airflow_webserver:/opt/airflow/output/tickers/. ./output/tickers/

🤖 Уведомления (Telegram)
TelegramAlertHook отправляет сообщения при:

✅ Успешном выполнении DAG (on_success_callback)

❌ Ошибке в DAG (on_failure_callback)

Настройка
bash

# Создание переменных в Airflow
docker exec -it airflow_webserver airflow variables set bot_token "YOUR_BOT_TOKEN"
docker exec -it airflow_webserver airflow variables set tg_chat_id "YOUR_CHAT_ID"
Проверка
bash
docker exec -it airflow_webserver python -c "
from hooks.telegram_hook import TelegramAlertHook
TelegramAlertHook().send_message('Тест из Airflow')
"

🌐 API источники данных

Источник	API	Данные	Документация
MOEX (Россия)	https://iss.moex.com/iss/	Акции SBER, GAZP, LKOH, ROSN, VTBR	MOEX ISS API
НБРБ (Беларусь)	https://api.nbrb.by/exrates	Курсы валют к BYN	НБРБ API

🧹 Управление проектом

Полный сброс и перезапуск
bash
docker compose down -v
docker compose up -d
Просмотр логов
bash
docker logs airflow_webserver --tail 50
docker logs airflow_scheduler --tail 50
docker logs fin_mongo --tail 30
Очистка кэша Airflow
bash
docker exec -it airflow_webserver rm -rf /opt/airflow/dags/__pycache__
docker exec -it airflow_webserver rm -rf /opt/airflow/dags/hooks/__pycache__
docker exec -it airflow_webserver rm -rf /opt/airflow/dags/operators/__pycache__
docker compose restart airflow_webserver airflow_scheduler
Обновление переменных Airflow
bash
docker exec -it airflow_webserver airflow variables import /path/to/variables.json

📌 Планы по развитию

Подключение реальных данных через yfinance

Добавление Superset (второй BI-инструмент)

Расширение списка российских акций

Добавление фундаментальных данных (Finnhub)

Оптимизация LSTM (Grid Search)

Деплой на облачную платформу (AWS/Azure)

📄 Лицензия
MIT License. Свободное использование, модификация и распространение.

👤 Автор
Vitaly Zosimov
Data Engineer Student

GitHub: @VitalyZosimov
