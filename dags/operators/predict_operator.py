from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from dags.hooks.mongo_hook import MongoHook
import pandas as pd
import numpy as np
from tensorflow.keras.models import load_model
import joblib
from datetime import datetime

class PredictOperator(BaseOperator):
		@apply_defaults
		def __init__(self, ticker='AAPL', sequence_length=60,
								 model_path='models/model.h5', scaler_path='models/scaler.pkl', **kwargs):
				super().__init__(**kwargs)
				self.ticker = ticker
				self.seq_len = sequence_length
				self.model_path = model_path
				self.scaler_path = scaler_path

		def execute(self, context):
				#Загружаем обученную модель и scaler
				model = load_model(self.model_path)
				scaler = joblib.load(self.scaler_path)

				#Подключаемся к MongoDB
				hook = MongoHook()
				db = hook.get_db('stockviz')

				#Берём последние `seq_len` записей для указанного тикера
				data = list(db.ticker_metrics.find({'ticker': self.ticker}).sort('date', -1).limit(self.seq_len))
				if len(data) < self.seq_len:
						raise ValueError(f"Недостаточно данных для {self.ticker}")
				df = pd.DataFrame(data).sort_values('date')   #Сортировка от старых к новым

				features = ['daily_return', 'volume_ratio', 'rsi_14', 'volatility_20d']

				#Нормируем признаки с помощью сохранённого scaler
				X_input = scaler.transform(df[features].values[-self.seq_len:])
				X_input = np.expand_dims(X_input, axis=0)   #Добавляем размерность для batch

				#Делаем предсказание
				pred = model.predict(X_input)[0, 0]

				#Сохраняем прогноз в отдельную коллекцию MongoDB (для истории)
				db.predictions.insert_one({
						'ticker': self.ticker,
						'date': datetime.utcnow().isoformat(),
						'predicted_volatility_20d': float(pred)
				})
				self.log.info(f"Предсказанная волатильность для {self.ticker}: {pred:.4f}")
				return pred   #Возвращаем значение (попадёт в XCom)