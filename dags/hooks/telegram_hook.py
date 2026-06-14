# Импорт модулей Airflow для переменных и базового хука
from airflow.models import Variable
from airflow.hooks.base import BaseHook
import logging
import requests

# Создаём логгер для этого модуля
logger = logging.getLogger(__name__)

class TelegramAlertHook(BaseHook):
		# Конструктор: берёт токен и chat_id из переменных Airflow
		def __init__(self, bot_token: str = Variable.get("bot_token"), chat_id: str = Variable.get("tg_chat_id")):
				super().__init__()
				self.bot_token = bot_token   # Токен бота Telegram
				self.chat_id = chat_id       # ID чата/пользователя

		# Метод отправки сообщения
		def send_message(self, text: str):
				# Если токен или chat_id не заданы, просто пишем в лог
				if not self.bot_token or not self.chat_id:
						logger.info("Не задан токен или чат айди")
						return
				# Формируем URL API Telegram
				url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
				# Отправляем POST-запрос с данными
				res = requests.post(url, data={"chat_id": self.chat_id, "text": text})
				# Проверяем статус ответа (выбросит исключение при ошибке)
				res.raise_for_status()
				# Печатаем код ответа (в логах Airflow будет видно)
				print(res.status_code)