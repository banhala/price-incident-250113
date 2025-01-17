import dataclasses
from datetime import datetime


@dataclasses.dataclass(frozen=True)
class Revision:
    goods_sno: int
    deal_sno: int
    goods_discount_policy_sno: int
    thumbnail_price: int
    consumer_origin: int
    price_origin: int
    platform_consumer: int
    discount_price: int
    discount_type: int
    is_wrong_price: bool
    transaction_time: datetime
