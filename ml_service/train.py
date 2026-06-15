from pymongo import MongoClient
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
import joblib
import os

client = MongoClient('mongodb://mongo:mongo@fin_mongo:27017/?authSource=admin')
db = client['stockviz']

tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

for ticker in tickers:
    print(f'\n=== Обработка {ticker} ===')
    
    data = list(db.stock_data.find({'ticker': ticker}, {'_id': 0, 'date': 1, 'close': 1}).sort('date', 1))
    df = pd.DataFrame(data)
    
    if len(df) < 100:
        print(f'Недостаточно данных для {ticker}, пропускаем')
        continue
    
    prices = df['close'].values.reshape(-1, 1)
    scaler = MinMaxScaler()
    prices_scaled = scaler.fit_transform(prices)
    
    seq_length = 60
    X, y = [], []
    for i in range(seq_length, len(prices_scaled)):
        X.append(prices_scaled[i-seq_length:i, 0])
        y.append(prices_scaled[i, 0])
    
    X = np.array(X).reshape(-1, seq_length, 1)
    y = np.array(y)
    
    split = int(0.8 * len(X))
    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y[:split], y[split:]
    
    model = Sequential([
        LSTM(50, return_sequences=True, input_shape=(seq_length, 1)),
        Dropout(0.2),
        LSTM(50, return_sequences=False),
        Dropout(0.2),
        Dense(25, activation='relu'),
        Dense(1)
    ])
    
    model.compile(optimizer='adam', loss='mse', metrics=['mae'])
    model.fit(X_train, y_train, epochs=20, batch_size=32, verbose=0)
    
    os.makedirs('/app/models', exist_ok=True)
    model.save(f'/app/models/lstm_{ticker}.h5')
    joblib.dump(scaler, f'/app/models/scaler_{ticker}.pkl')
    print(f'Модель для {ticker} сохранена')