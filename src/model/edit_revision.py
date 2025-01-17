import dataclasses
from datetime import datetime

from src.model.option_context import GoodsContext
from src.model.raw_csv import RawCsvDeal


@dataclasses.dataclass(frozen=True)
class EditRevision:
    goods_sno: int
    context: GoodsContext
    deal: RawCsvDeal
    transaction_time: datetime


@dataclasses.dataclass(frozen=True)
class EditRevisionRow:
    goods_sno: int
    thumbnail_price: int
    consumer_origin: int
    price_origin: int
    platform_consumer: int
    discount_price: int
    discount_value: int
    transaction_time: datetime
