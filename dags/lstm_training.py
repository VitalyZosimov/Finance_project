from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from hooks.mongo_hook import MongoHook
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
import os

def train_lstm():
    # Подключаемся к MongoDB
    hook = MongoHook()
    db = hook.get_db('stockviz')
    
    # Загружаем данные AAPL
    data = list(db.stock_data.find({'ticker': 'AAPL'}, {'_id': 0, 'date': 1, 'close': 1}).sort('date', 1))
    df = pd.DataFrame(data)
    
    if len(df) < 100:
        raise ValueError(f'Недостаточно данных: {len(df)} записей')
    
    print(f'Загружено {len(df)} записей для AAPL')
    
    # Берём цены закрытия
    prices = df['close'].values.reshape(-1, 1)
    
    # Нормализация
    scaler = MinMaxScaler()
    prices_scaled = scaler.fit_transform(prices)
    
    # Создаём последовательности для LSTM
    seq_length = 60
    X, y = [], []
    for i in range(seq_length, len(prices_scaled)):
        X.append(prices_scaled[i-seq_length:i, 0])
        y.append(prices_scaled[i, 0])
    
    X = np.array(X).reshape(-1, seq_length, 1)
    y = np.array(y)
    
    print(f'X shape: {X.shape}, y shape: {y.shape}')
    
    # Разделяем на train/test
    split = int(0.8 * len(X))
    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y[:split], y[split:]
    
    # Строим LSTM модель
    model = Sequential([
        LSTM(50, return_sequences=True, input_shape=(seq_length, 1)),
        Dropout(0.2),
        LSTM(50, return_sequences=False),
        Dropout(0.2),
        Dense(25, activation='relu'),
        Dense(1)
    ])
    
    model.compile(optimizer='adam', loss='mse', metrics=['mae'])
    
    # Обучение
    history = model.fit(
        X_train, y_train,
        epochs=30,
        batch_size=32,
        validation_data=(X_test, y_test),
        verbose=1
    )
    
    # Сохраняем модель
    os.makedirs('/opt/airflow/models', exist_ok=True)
    model.save('/opt/airflow/models/lstm_aapl.h5')
    import joblib
    joblib.dump(scaler, '/opt/airflow/models/scaler_aapl.pkl')
    
    print('✅ Модель обучена и сохранена!')
    return 'OK'

with DAG(
    dag_id='lstm_training',
    start_date=datetime(2020, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    train = PythonOperator(
        task_id='train_lstm',
        python_callable=train_lstm
    )