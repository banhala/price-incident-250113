import copy
import csv
import dataclasses
from collections import defaultdict
from datetime import datetime, timedelta
from io import StringIO

from file_save_helper import FileSaveHelper
from src.util import discard_ones_digit


@dataclasses.dataclass(frozen=False)
class GoodsContext:
    goods_sno: int = -1
    goods_name: str = ''
    thumbnail_price: int = -1
    correct_price: int = -1
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


@dataclasses.dataclass(frozen=True)
class InvalidData:
    context: GoodsContext
    log: DatadogLog
    item: OrderItem


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
        return sorted(res, key=lambda x: x.checked_at)


class ItemAnalyzer:
    @classmethod
    def analyze(cls, logs: list[DatadogLog], items: list[OrderItem]) -> list[InvalidData]:
        res: list[InvalidData] = []
        context_map: dict[int, GoodsContext] = defaultdict(GoodsContext)
        goods_map: dict[int, list[DatadogLog]] = defaultdict(list)
        for log in logs:
            goods_map[log.goods_sno].append(log)

        for item in items:
            checked_at: datetime = item.checked_at
            ctx = cls._apply_updates(
                ctx=context_map[item.goods_sno], logs=goods_map[item.goods_sno], checked_at=checked_at
            )
            ctx.goods_name = item.goods_name
            context_map[item.goods_sno] = ctx
            if ctx.correct_price != item.price and ctx.goods_sno != -1:
                res.append(
                    InvalidData(
                        context=copy.deepcopy(ctx),
                        log=copy.deepcopy(log),
                        item=copy.deepcopy(item),
                    )
                )

        return res

    @classmethod
    def _apply_updates(cls, ctx: GoodsContext, logs: list[DatadogLog], checked_at: datetime) -> GoodsContext:
        while logs and logs[0].request_time <= checked_at:
            log: DatadogLog = logs.pop(0)
            correct_price: int = cls.calc_correct_price(log=log)
            ctx.goods_sno = log.goods_sno
            ctx.correct_price = correct_price
            ctx.price_origin = log.price_origin
            ctx.consumer_origin = log.consumer_origin
            ctx.discount_price = log.discount_price
            ctx.discount_rate = log.discount_rate
            ctx.discount_type = log.discount_type
            ctx.updated_at = log.request_time
            ctx.goods_sno = log.goods_sno
        return ctx

    @classmethod
    def calc_correct_price(cls, log: DatadogLog) -> int:
        '''
        algorithm.

        정가: [max(consumer_origin, price_origin)]
        할인: [
            정가 * discount_rate,
            discount_price,
            consumer_diff,
        ]
        '''
        consumer: int = cls._get_best_consumer(
            consumer_origin=log.consumer_origin,
            price_origin=log.price_origin,
        )
        discount: int = cls._get_best_discount(
            consumer=consumer,
            discount_price=log.discount_price,
            discount_type=log.discount_type,
            discount_rate=log.discount_rate,
            diff=abs(log.consumer_origin - log.price_origin),
            started_at=log.discount_started_at,
            ended_at=log.discount_ended_at,
            checked_at=log.request_time,
        )
        return consumer - discount

    @classmethod
    def _get_best_consumer(cls, consumer_origin: int, price_origin: int) -> int:
        return max([consumer_origin, price_origin])

    @classmethod
    def _get_best_discount(
            cls,
            consumer: int,
            discount_price: int,
            discount_type: int,
            discount_rate: int,
            diff: int,
            started_at: datetime,
            ended_at: datetime,
            checked_at: datetime,
    ) -> int:
        adj_discount_price: int = cls._get_consumer_price_adj_discount(
            consumer=consumer,
            discount_price=discount_price,
            discount_type=discount_type,
            discount_rate=discount_rate,
            started_at=started_at,
            ended_at=ended_at,
            checked_at=checked_at,
        )
        return adj_discount_price or diff

    @classmethod
    def _get_consumer_price_adj_discount(
            cls,
            consumer: int,
            discount_price: int,
            discount_type: int,
            discount_rate: int,
            started_at: datetime,
            ended_at: datetime,
            checked_at: datetime,
    ) -> int:
        can_discount: bool = started_at <= checked_at <= ended_at
        if discount_type == 1 and discount_rate > 0 and discount_price == 0:
            # 정률
            res = discard_ones_digit(
                consumer * discount_rate
            )
            if can_discount:
                return res if res > 0 else 0

        if can_discount:
            # 단가
            res = discard_ones_digit(discount_price if discount_price > 0 else 0)
            return res

        return 0


class DataPrinter:
    @classmethod
    def print(cls, data: list[InvalidData], goods_set: set[int]) -> None:
        # Print header
        print("\n" + "=" * 150)
        print(
            f"{'Goods Name':<40} | {'SNO':>8} | {'Price':>10} | {'Correct':>10} | {'Diff':>8} | {'Checked At':^20} | {'Updated At':^20}")
        print("-" * 150)

        # Print each row
        for invalid in data:
            if invalid.context.goods_sno not in goods_set:
                continue

            goods_name: str = invalid.context.goods_name[:37] + "..." if len(
                invalid.context.goods_name) > 40 else invalid.context.goods_name
            goods_sno: int = invalid.context.goods_sno
            price: int = invalid.item.price
            correct_price: int = invalid.context.correct_price
            price_diff: int = correct_price - price
            checked_at: datetime = invalid.item.checked_at
            updated_at: datetime = invalid.context.updated_at

            print(
                f"{goods_name:<40} | "
                f"{goods_sno:>8} | "
                f"{price:>10,d} | "
                f"{correct_price:>10,d} | "
                f"{price_diff:>8,d} | "
                f"{checked_at.strftime('%Y-%m-%d %H:%M')} | "
                f"{updated_at.strftime('%Y-%m-%d %H:%M')}"
            )

        print("=" * 150 + "\n")

    @classmethod
    def map_csv(cls, data: list[InvalidData]) -> bytes:
        # Define headers
        headers = [
            'goods_name',
            'goods_sno',
            'price',
            'correct_price',
            'price_diff',
            'checked_at',
            'updated_at',
            'market_sno',
            'order_sno',
            'item_sno'
        ]

        # Create CSV content starting with headers
        lines = [','.join(headers)]

        # Add data rows
        for invalid in data:
            row = [
                f'"{invalid.context.goods_name}"',  # Quote the name to handle commas
                str(invalid.context.goods_sno),
                str(invalid.item.price),
                str(invalid.context.correct_price),
                str(invalid.context.correct_price - invalid.item.price),
                invalid.item.checked_at.strftime('%Y-%m-%d %H:%M:%S'),
                invalid.context.updated_at.strftime('%Y-%m-%d %H:%M:%S'),
                str(invalid.item.market_sno),
                str(invalid.item.order_sno),
                str(invalid.item.item_sno)
            ]
            lines.append(','.join(row))

        # Join with newlines and encode to bytes
        return '\n'.join(lines).encode('utf-8')


if __name__ == '__main__':
    print('program starts to parse..')
    logs: list[DatadogLog] = LogReader.parse(
        lines=LogReader.read(filepaths=['data/spainshop1.csv', 'data/spainshop2.csv']),
    )
    items: list[OrderItem] = ItemReader.parse(lines=ItemReader.read(filepaths=['data/spain_items.csv']))
    invalids: list[InvalidData] = ItemAnalyzer.analyze(logs=logs, items=items)

    print('len(logs): ', len(logs))
    print('len(items): ', len(items))
    print('len(invalids): ', len(invalids))
    print('unique goods: ', len(set([invalid.context.goods_sno for invalid in invalids])))
    FileSaveHelper.save(data=DataPrinter.map_csv(data=invalids), filepath='data/invalids_spainshop.csv')
    DataPrinter.print(data=invalids, goods_set={29171090})
