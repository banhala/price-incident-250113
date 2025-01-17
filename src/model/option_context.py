import dataclasses
from datetime import datetime


@dataclasses.dataclass(frozen=False)
class OptionContext:
    goods_sno: int = -1
    option_sno: int = -1
    consumer_origin: int = -1
    price_origin: int = -1
    total_additional_price: int = -1
    created_at: datetime = datetime(1970, 1, 1)
    updated_at: datetime = datetime(1970, 1, 1)


@dataclasses.dataclass(frozen=False)
class GoodsContext:
    goods_sno: int = -1
    thumbnail_price: int = -1
    consumer_origin: int = -1
    price_origin: int = -1
    total_additional_price: int = -1
    platform_consumer: int = -1
    platform_total_additional_price: int = -1
    discount_price: int = -1
    discount_type: int = -1
    created_at: datetime = datetime(1970, 1, 1)
    updated_at: datetime = datetime(1970, 1, 1)
