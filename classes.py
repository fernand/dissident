from pydantic import BaseModel

from dataclasses import dataclass

@dataclass
class TickerInfo:
    ticker: str
    close: float
    exchange: str | None
    cik: str | None
    type: str | None
    active: bool
    market_cap: int | None

@dataclass
class NullTickerInfo:
    ticker: str

class CEO(BaseModel):
    name: str
    is_founder: bool
