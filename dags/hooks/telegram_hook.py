from airflow.models import Variable
from airflow.hooks.base import BaseHook
import logging
import requests

logger = logging.getLogger(__name__)

class TelegramAlertHook(BaseHook):
    def __init__(self, bot_token=None, chat_id=None):
        super().__init__()
        self.bot_token = bot_token
        self.chat_id = chat_id

    def send_message(self, text: str):
        token = self.bot_token or Variable.get('bot_token', default_var=None)
        chat = self.chat_id or Variable.get('tg_chat_id', default_var=None)

        if not token or not chat:
            self.log.info('Не задан токен или chat_id Telegram — пропускаем отправку')
            return

        url = f'https://api.telegram.org/bot{token}/sendMessage'
        try:
            res = requests.post(url, data={'chat_id': chat, 'text': text}, timeout=10)
            res.raise_for_status()
            self.log.info(f'Сообщение в Telegram отправлено: {res.status_code}')
        except Exception as e:
            self.log.error(f'Ошибка при отправке в Telegram: {e}')