from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import List, Optional

class MoexStock(BaseModel):
    """
    Модель для одной записи о российской акции (MOEX)
    """
    begin: datetime = Field(..., description="Дата и время начала свечи")
    ticker: str = Field(..., min_length=1, max_length=10, description="Тикер")
    open: float = Field(..., ge=0, description="Цена открытия")
    close: float = Field(..., ge=0, description="Цена закрытия")
    high: float = Field(..., ge=0, description="Максимум")
    low: float = Field(..., ge=0, description="Минимум")
    volume: int = Field(..., ge=0, description="Объём торгов")
    
    @field_validator('ticker')
    @classmethod
    def validate_ticker(cls, v: str) -> str:
        v = v.upper().strip()
        if not v.isalnum():
            raise ValueError(f'Некорректный тикер MOEX: {v}')
        return v
    
    @field_validator('close')
    @classmethod
    def validate_price(cls, v: float) -> float:
        if v <= 0:
            raise ValueError(f'Цена должна быть положительной: {v}')
        return round(v, 2)
    
    @field_validator('high', 'low')
    @classmethod
    def validate_high_low(cls, v: float, info) -> float:
        if v <= 0:
            raise ValueError(f'{info.field_name} должен быть положительным: {v}')
        return round(v, 2)
    
    @field_validator('volume')
    @classmethod
    def validate_volume(cls, v: int) -> int:
        if v < 0:
            raise ValueError(f'Объём не может быть отрицательным: {v}')
        return v

class MoexStockList(BaseModel):
    """Список записей о российских акциях"""
    data: List[MoexStock]
    
    @property
    def total_count(self) -> int:
        return len(self.data)