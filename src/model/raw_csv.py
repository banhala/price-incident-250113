import dataclasses
from datetime import datetime


@dataclasses.dataclass(frozen=True)
class RawCsvGoodsOption:
    market_sno: int
    goods_sno: int
    option_sno: int
    consumer_origin: int
    price_origin: int
    total_additional_price: int
    link: int
    is_display: int
    operation_type: str
    transaction_time: datetime
    dt: str


@dataclasses.dataclass(frozen=True)
class RawCsvPolicy:
    market_sno: int
    goods_sno: int
    is_active: bool
    status: int
    discount_method: int
    pricing_strategy: int
    policy_type: int
    policy_value: int
    # app_type: list[int]
    started_at: datetime
    ended_at: datetime
    operation_type: str
    transaction_time: datetime
    dt: str


@dataclasses.dataclass(frozen=True)
class RawCsvDeal:
    sno: int
    goods_sno: int
    goods_discount_policy_sno: int
    thumbnail_price: int
    is_enabled: bool
    priority: int
    app_type: int
    started_at: datetime
    ended_at: datetime
    operation_type: str
    deleted: bool
    transaction_time: datetime
    dt: str
