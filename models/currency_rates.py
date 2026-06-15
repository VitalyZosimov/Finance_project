from pydantic import BaseModel, Field, field_validator
from datetime import date
from typing import List

class CurrencyRate(BaseModel):
    """
    Модель для одной записи о курсе валюты
    """
    date: date = Field(..., description="Дата курса")
    currency: str = Field(..., min_length=3, max_length=3, description="Код валюты (ISO 4217)")
    rate_to_byn: float = Field(..., ge=0, description="Курс к BYN")
    scale: int = Field(1, ge=1, le=1000, description="Масштаб валюты")
    
    @field_validator('currency')
    @classmethod
    def validate_currency(cls, v: str) -> str:
        v = v.upper().strip()
        valid_currencies = ['USD', 'EUR', 'RUB', 'CNY', 'JPY', 'GBP', 'PLN', 'UAH', 'BYN']
        if v not in valid_currencies:
            raise ValueError(f'Некорректный код валюты: {v}. Допустимы: {valid_currencies}')
        return v
    
    @field_validator('rate_to_byn')
    @classmethod
    def validate_rate(cls, v: float) -> float:
        if v < 0:
            raise ValueError(f'Курс не может быть отрицательным: {v}')
        return round(v, 4)
    
    @field_validator('scale')
    @classmethod
    def validate_scale(cls, v: int) -> int:
        if v <= 0:
            raise ValueError(f'Масштаб должен быть положительным: {v}')
        return v
    
    @property
    def actual_rate(self) -> float:
        """Возвращает фактический курс с учётом масштаба"""
        return self.rate_to_byn / self.scale

class CurrencyRateList(BaseModel):
    """Список курсов валют"""
    data: List[CurrencyRate]
    
    @property
    def total_count(self) -> int:
        return len(self.data)
    
    def get_rate(self, currency: str) -> float:
        """Получить курс для конкретной валюты"""
        for item in self.data:
            if item.currency == currency:
                return item.actual_rate
        raise ValueError(f'Валюта {currency} не найдена')