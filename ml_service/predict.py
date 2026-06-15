from pymongo import MongoClient
import numpy as np
from tensorflow.keras.models import load_model
import joblib
from datetime import datetime

client = MongoClient('mongodb://mongo:mongo@fin_mongo:27017/?authSource=admin')
db = client['stockviz']

tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

for ticker in tickers:
    print(f'\\n=== Прогноз для {ticker} ===')
    
    try:
        model = load_model(f'/app/models/lstm_{ticker}.h5')
        scaler = joblib.load(f'/app/models/scaler_{ticker}.pkl')
    except:
        print(f'Модель для {ticker} не найдена, пропускаем')
        continue
    
    # Берём последние 60 дней
    data = list(db.stock_data.find({'ticker': ticker}, {'_id': 0, 'close': 1}).sort('date', -1).limit(60))
    if len(data) < 60:
        print(f'Недостаточно данных для {ticker}')
        continue
    
    # Последняя известная цена
    last_price = data[0]['close']
    
    prices = np.array([d['close'] for d in data])[::-1].reshape(-1, 1)
    prices_scaled = scaler.transform(prices)
    X = prices_scaled.reshape(1, 60, 1)
    
    pred_scaled = model.predict(X, verbose=0)[0][0]
    pred_price = scaler.inverse_transform([[pred_scaled]])[0][0]
    
    print(f'Последняя цена: {last_price:.2f}')
    print(f'Прогноз цены: {pred_price:.2f}')
    print(f'Изменение: {((pred_price - last_price) / last_price * 100):.2f}%')
    
    db.predictions.update_one(
        {'ticker': ticker, 'date': datetime.now().strftime('%Y-%m-%d')},
        {'$set': {
            'last_price': round(last_price, 2),
            'predicted_close': round(pred_price, 2),
            'change_percent': round((pred_price - last_price) / last_price * 100, 2),
            'model': 'LSTM',
            'updated_at': datetime.now()
        }},
        upsert=True
    )

print('\\n Все прогнозы сохранены в MongoDB')