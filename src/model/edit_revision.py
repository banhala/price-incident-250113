import dataclasses


@dataclasses.dataclass(frozen=True)
class EditRevision:
    goods_sno: int
    market_sno: int


@dataclasses.dataclass(frozen=True)
class CorrectPriceRevision:
    market_sno: int
    goods_sno: int
    goods_option_sno: int
    price_origin: int


# 1. 미래에 시작할 가격은 어떻게 검증할까?
@dataclasses.dataclass(frozen=True)
class WrongPriceRevision:
    market_sno: int
    goods_sno: int
    goods_option_sno: int
    thumbnail_price: int
