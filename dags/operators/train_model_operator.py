from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from dags.hooks.mongo_hook import MongoHook
import pandas as pd
import numpy as np
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from sklearn.preprocessing import MinMaxScaler
import joblib
import os

class TrainModelOperator(BaseOperator):
		@apply_defaults
		def __init__(self, ticker='AAPL', sequence_length=60,
								 model_save_path='models/model.h5', scaler_save_path='models/scaler.pkl', **kwargs):
				super().__init__(**kwargs)
				self.ticker = ticker                      #Тикер для обучения
				self.seq_len = sequence_length            #Длина последовательности для LSTM
				self.model_path = model_save_path         #Куда сохранить модель
				self.scaler_path = scaler_save_path       #Куда сохранить scaler

		def pre_execute(self, context):
				
				#Создаём папку для модели, если её нет
				os.makedirs(os.path.dirname(self.model_path), exist_ok=True)

		def execute(self, context):
				#1 Загружаем данные из MongoDB (коллекция ticker_metrics)
				hook = MongoHook()
				db = hook.get_db('stockviz')
				data = list(db.ticker_metrics.find({'ticker': self.ticker}))
				if not data:
						raise ValueError(f"Нет данных для {self.ticker}")
				df = pd.DataFrame(data)
				df['date'] = pd.to_datetime(df['date'])
				df = df.sort_values('date')

				#2 Отбираем признаки и целевую переменную (будущая волатильность через 20 дней)
				features = ['daily_return', 'volume_ratio', 'rsi_14', 'volatility_20d']
				df = df.dropna(subset=features + ['volatility_20d'])
				df['target'] = df['volatility_20d'].shift(-20)
				df = df.dropna()

				#3 Нормировка признаков
				scaler = MinMaxScaler()
				scaled = scaler.fit_transform(df[features])

				#4 Формируем последовательности для LSTM (X) и целевые значения (y)
				X, y = [], []
				for i in range(self.seq_len, len(df)):
						X.append(scaled[i-self.seq_len:i])
						y.append(df['target'].iloc[i])
				X, y = np.array(X), np.array(y)

				#5 Разделяем на обучение (80%) и тест (20%)
				split = int(0.8 * len(X))
				X_train, X_test = X[:split], X[split:]
				y_train, y_test = y[:split], y[split:]

				#6 Построение модели LSTM с дополнительным плотным слоем ReLU
				model = Sequential([

						#Первый LSTM слой, возвращает последовательности (для следующего LSTM)
						LSTM(50, return_sequences=True, input_shape=(self.seq_len, len(features))),
						Dropout(0.2),                         # Регуляризация

						#Второй LSTM слой, возвращает только последний выход
						LSTM(50, return_sequences=False),
						Dropout(0.2),

						#Дополнительный полносвязный слой с ReLU (улучшает нелинейность)
						Dense(25, activation='relu'),

						#Выходной слой (прогноз волатильности)
						Dense(1)
				])

				#Компиляция: оптимизатор Adam, функция потерь MSE, метрика MAE
				model.compile(optimizer='adam', loss='mse', metrics=['mae'])

				#8 Обучение
				model.fit(X_train, y_train, epochs=30, batch_size=16, #Для быстроты можно уменьшить epochs до 5 (<5 результата нету, очень много - нет обучаемости)
									validation_data=(X_test, y_test), verbose=1)

				#8 Сохраняем модель и scaler
				model.save(self.model_path)
				joblib.dump(scaler, self.scaler_path)
				self.log.info(f"Модель сохранена в {self.model_path}")

				#Возвращаем пути (для XCom, если нужно)
				return {'model_path': self.model_path, 'scaler_path': self.scaler_path}