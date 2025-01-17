import csv
import dataclasses
from datetime import datetime, timedelta
from io import StringIO

from csv_parser import CsvParser
from file_save_helper import FileSaveHelper


@dataclasses.dataclass(frozen=True)
class GoodsContext:
    goods_sno: int = -1
    thumbnail_price: int = -1
    consumer_origin: int = -1
    price_origin: int = -1
    discount_type: int = -1
    discount_rate: int = -1
    discount_price: int = -1
    updated_at: datetime = datetime(1970, 1, 1)


@dataclasses.dataclass(frozen=True)
class DatadogLog:
    market_sno: int
    goods_sno: int
    consumer_origin: int
    price_origin: int
    discount_type: int
    discount_rate: int
    discount_price: int
    discount_started_at: datetime
    discount_ended_at: datetime
    request_time: datetime


@dataclasses.dataclass(frozen=True)
class OrderItem:
    item_sno: int
    order_sno: int
    market_sno: int
    goods_sno: int
    goods_name: str
    quantity: int
    price: int
    checked_at: datetime


class ReaderUtil:
    @classmethod
    def parse_value(cls, value: str) -> str | int:
        """Parse a CSV value that might be wrapped in quotes or be a plain integer."""
        # Strip quotes if present
        if value.startswith('"') and value.endswith('"'):
            return value[1:-1]
        return value

    @classmethod
    def parse_int(cls, value: str) -> int:
        """Parse a value into an integer, handling quoted strings."""
        cleaned_value = cls.parse_value(value)
        return int(cleaned_value)

    @classmethod
    def parse_csv_line_with_csv(cls, line: str) -> list[str]:
        return next(csv.reader(StringIO(line)))


class LogReader:
    HEADERS: list[str] = [
        "Date",
        "ably_market_sno",
        "@ably_sno",
        "consumer_origin",
        "@price_origin",
        "@consumer_price_adjustment.discount_type",
        "@consumer_price_adjustment.discount_rate",
        "@consumer_price_adjustment.discount_price",
        "@consumer_price_adjustment.ended_at",
        "@consumer_price_adjustment.started_at",
        "Message",
    ]

    @classmethod
    def read(cls, filepaths: list[str]) -> list[str]:
        res: list[str] = []
        for filepath in filepaths:
            data: bytes = FileSaveHelper.read(filepath=filepath)
            decoded = data.decode('utf-8')
            lines = decoded.splitlines()

            # Skip header
            res.extend(lines[1:])

        return res

    @classmethod
    def parse(cls, lines: list[str]) -> list[DatadogLog]:
        res: list[DatadogLog] = []
        for line in lines:
            columns: list[str] = ReaderUtil.parse_csv_line_with_csv(line=line)
            res.append(cls._parse_csv_line(line=columns))
        return sorted(res, key=lambda x: x.request_time)

    @classmethod
    def _parse_csv_line(cls, line: list[str]) -> DatadogLog:
        dt: datetime = datetime.fromisoformat(line[0].replace('Z', '+00:00')) + timedelta(hours=9)
        started_at: datetime = (
            datetime.fromisoformat(ReaderUtil.parse_value(line[9]).replace('Z', '+00:00'))
            if line[9] else datetime(1970, 1, 1)
        )
        ended_at: datetime = (
            datetime.fromisoformat(ReaderUtil.parse_value(line[8]).replace('Z', '+00:00'))
            if line[8] else datetime(9999, 12, 31)
        )
        return DatadogLog(
            market_sno=ReaderUtil.parse_int(line[1]),
            goods_sno=ReaderUtil.parse_int(line[2]),
            consumer_origin=ReaderUtil.parse_int(line[3]),
            price_origin=ReaderUtil.parse_int(line[4]),
            discount_type=ReaderUtil.parse_int(line[5]) if line[5] else -1,
            discount_rate=ReaderUtil.parse_int(line[6]) if line[6] else -1,
            discount_price=ReaderUtil.parse_int(line[7]) if line[7] else -1,
            discount_started_at=datetime(
                year=started_at.year,
                month=started_at.month,
                day=started_at.day,
                hour=started_at.hour,
                minute=started_at.minute,
                second=started_at.second,
                microsecond=started_at.microsecond,
            ),
            discount_ended_at=datetime(
                year=ended_at.year,
                month=ended_at.month,
                day=ended_at.day,
                hour=ended_at.hour,
                minute=ended_at.minute,
                second=ended_at.second,
                microsecond=ended_at.microsecond,
            ),
            request_time=datetime(
                year=dt.year,
                month=dt.month,
                day=dt.day,
                hour=dt.hour,
                minute=dt.minute,
                second=dt.second,
                microsecond=dt.microsecond,
            ),
        )


class ItemReader:
    @classmethod
    def read(cls, filepaths: list[str]) -> list[str]:
        res: list[str] = []
        for filepath in filepaths:
            data: bytes = FileSaveHelper.read(filepath=filepath)
            decoded = data.decode('utf-8')
            lines = decoded.splitlines()

            # Skip header
            res.extend(lines[1:])

        return res

    @classmethod
    def parse(cls, lines: list[str]) -> list[OrderItem]:
        '''
        sno, 0
        ordno, 1
        market_sno, 2
        goodsno, 3
        goodsnm, 4
        price, 5
        memberdc,
        emoney,
        coupon,
        ea, 9
        reserve,
        checked_at 11
        '''
        res: list[OrderItem] = []
        for line in lines:
            columns: list[str] = ReaderUtil.parse_csv_line_with_csv(line=line)
            checked_at: datetime = datetime.strptime(columns[11], "%Y-%m-%d %H:%M:%S.%f")
            res.append(
                OrderItem(
                    item_sno=ReaderUtil.parse_int(columns[0]),
                    order_sno=ReaderUtil.parse_int(columns[1]),
                    market_sno=int(columns[2]),
                    goods_sno=ReaderUtil.parse_int(columns[3]),
                    goods_name=columns[4],
                    price=ReaderUtil.parse_int(columns[5]),
                    quantity=ReaderUtil.parse_int(columns[9]),
                    checked_at=checked_at,
                )
            )
        return res


if __name__ == '__main__':
    print('program starts to parse..')
    logs: list[DatadogLog] = LogReader.parse(
        lines=LogReader.read(filepaths=['data/spainshop1.csv', 'data/spainshop2.csv']),
    )
    for log in logs[:5]:
        print(log)
    print('len(logs): ', len(logs))

    items: list[OrderItem] = ItemReader.parse(lines=ItemReader.read(filepaths=['data/spain_items.csv']))
    for item in items:
        print(item)
    print('len(items): ', len(items))