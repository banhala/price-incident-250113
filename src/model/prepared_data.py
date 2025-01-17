import dataclasses

from src.model.raw_csv import RawCsvDeal, RawCsvGoodsOption, RawCsvPlatformConsumer, RawCsvAdj


@dataclasses.dataclass(frozen=True)
class PreparedData:
    deal_map: dict[int, list[RawCsvDeal]]
    option_map: dict[int, list[RawCsvGoodsOption]]
    platform_consumer_map: dict[int, list[RawCsvPlatformConsumer]]
    adj_map: dict[int, list[RawCsvAdj]]
