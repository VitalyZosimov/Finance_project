from pydantic import BaseModel, Field, field_validator
from datetime import date
from typing import List, Optional

class StockData(BaseModel):
    """
    Модель для одной записи о цене акции
    """
    date: date = Field(..., description="Дата торгов")
    ticker: str = Field(..., min_length=1, max_length=10, description="Тикер акции")
    close: float = Field(..., ge=0, le=10000, description="Цена закрытия")
    volume: int = Field(..., ge=0, le=10**12, description="Объём торгов")
    
    # Опциональные поля (могут отсутствовать в некоторых данных)
    open: Optional[float] = Field(None, ge=0, le=10000, description="Цена открытия")
    high: Optional[float] = Field(None, ge=0, le=10000, description="Максимум")
    low: Optional[float] = Field(None, ge=0, le=10000, description="Минимум")
    
    @field_validator('ticker')
    @classmethod
    def validate_ticker(cls, v: str) -> str:
        """Валидация тикера: только заглавные буквы и цифры"""
        v = v.upper().strip()
        if not v.replace('.', '').isalnum():
            raise ValueError(f'Некорректный тикер: {v}. Допустимы буквы, цифры и точка')
        return v
    
    @field_validator('close')
    @classmethod
    def validate_positive_price(cls, v: float) -> float:
        """Цена должна быть положительной"""
        if v <= 0:
            raise ValueError(f'Цена должна быть положительной: {v}')
        return round(v, 2)
    
    @field_validator('volume')
    @classmethod
    def validate_volume(cls, v: int) -> int:
        """Объём должен быть неотрицательным целым числом"""
        if v < 0:
            raise ValueError(f'Объём не может быть отрицательным: {v}')
        return v

class StockDataList(BaseModel):
    """Список записей о ценах акций"""
    data: List[StockData]
    
    @property
    def total_count(self) -> int:
        return len(self.data)
    
    @property
    def tickers_list(self) -> List[str]:
        return list(set([item.ticker for item in self.data]))