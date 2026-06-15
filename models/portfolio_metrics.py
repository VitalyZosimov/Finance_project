from pydantic import BaseModel, Field, field_validator
from datetime import date
from typing import List, Optional

class PortfolioMetrics(BaseModel):
    """
    Модель для одной записи портфельной метрики
    """
    date: date = Field(..., description="Дата")
    portfolio_value: float = Field(..., ge=0, description="Стоимость портфеля")
    portfolio_return: Optional[float] = Field(None, description="Доходность портфеля за день")
    
    @field_validator('portfolio_value')
    @classmethod
    def validate_positive_value(cls, v: float) -> float:
        if v <= 0:
            raise ValueError(f'Стоимость портфеля должна быть положительной: {v}')
        return round(v, 2)
    
    @field_validator('portfolio_return')
    @classmethod
    def validate_return(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and (v < -1 or v > 10):
            # Доходность не может быть меньше -100% или больше 1000% (разумный диапазон)
            raise ValueError(f'Некорректная доходность: {v}')
        return round(v, 6) if v is not None else None

class PortfolioMetricsList(BaseModel):
    """Список портфельных метрик"""
    data: List[PortfolioMetrics]
    
    @property
    def total_count(self) -> int:
        return len(self.data)
    
    @property
    def last_value(self) -> Optional[float]:
        if self.data:
            return self.data[-1].portfolio_value
        return None