import dataclasses
from datetime import datetime

from src.model.option_context import OptionContext
from src.model.raw_csv import RawCsvDeal


@dataclasses.dataclass(frozen=True)
class EditRevision:
    goods_sno: int
    context: OptionContext
    deal: RawCsvDeal
    transaction_time: datetime
