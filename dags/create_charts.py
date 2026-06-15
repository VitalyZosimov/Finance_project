import os
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# --- 1. Убедимся, что папка для графиков существует ---
output_dir = '/home/airflow/output'
os.makedirs(output_dir, exist_ok=True)
print(f'Папка для графиков: {output_dir}')

# --- 2. Создаём тестовые данные ---
dates = pd.date_range(start='2020-01-01', end='2025-12-31', freq='D')
# Симулируем рост портфеля на 0.05% в день + случайные колебания
portfolio_value = 100000 * (1 + 0.0005 * np.arange(len(dates))) + np.random.randn(len(dates)) * 500
portfolio_return = np.random.randn(len(dates)) * 0.01

df = pd.DataFrame({
    'date': dates,
    'portfolio_value': portfolio_value,
    'portfolio_return': portfolio_return
})
print(f'Создано {len(df)} записей')

# --- 3. Создаём и сохраняем графики ---
print('Создаём графики...')

fig1 = px.line(df, x='date', y='portfolio_value', title='Стоимость портфеля')
fig1.write_html(os.path.join(output_dir, 'portfolio_value.html'))
print('  - Стоимость портфеля (portfolio_value.html)')

fig2 = px.line(df, x='date', y='portfolio_return', title='Доходность портфеля')
fig2.write_html(os.path.join(output_dir, 'portfolio_return.html'))
print('  - Доходность портфеля (portfolio_return.html)')

fig3 = px.histogram(df, x='portfolio_return', nbins=50, title='Распределение доходности')
fig3.write_html(os.path.join(output_dir, 'portfolio_histogram.html'))
print('  - Распределение доходности (portfolio_histogram.html)')

# Расчёт просадки
df['cummax'] = df['portfolio_value'].cummax()
df['drawdown'] = (df['portfolio_value'] - df['cummax']) / df['cummax'] * 100
fig4 = go.Figure()
fig4.add_trace(go.Scatter(x=df['date'], y=df['drawdown'], fill='tozeroy', name='Просадка, %'))
fig4.update_layout(title='Просадка портфеля', yaxis_title='Просадка (%)')
fig4.write_html(os.path.join(output_dir, 'drawdown.html'))
print('  - Просадка портфеля (drawdown.html)')

print(f'\nГотово! Все файлы сохранены в {output_dir}')