import copy
import csv
import dataclasses
import json
import os
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
    option_sno: int
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
    def parse_value(cls, value: str) -> str:
        """Parse a CSV value that might be wrapped in quotes or be a plain integer."""
        # Strip quotes if present
        if value.startswith('"') and value.endswith('"'):
            return value[1:-1]
        return value

    @classmethod
    def parse_int(cls, value: str) -> int:
        """Parse a value into an integer, handling quoted strings."""
        if value == '':
            return -1

        cleaned_value: str = cls.parse_value(value)
        try:
            return int(cleaned_value.replace("'", ""))
        except ValueError:
            return int(float(cleaned_value))

    @classmethod
    def parse_csv_line_with_csv(cls, line: str) -> list[str]:
        return next(csv.reader(StringIO(line)))


class GrafanaLogReader:
    @classmethod
    def read(cls, filepaths: list[str], limit: int | None) -> list[str]:
        print('grafana read log starts...')
        res: list[str] = []
        for filepath in filepaths:
            data: bytes = FileSaveHelper.read(filepath=filepath)
            print('bytes loaded..')
            decoded = data.decode('utf-8')
            if limit:
                lines: list[str] = decoded.splitlines()[:limit]
            else:
                lines: list[str] = decoded.splitlines()

            print('lines: ', len(lines))
            for line in lines:
                res.append(cls._extract_log_data(line))

        return res

    @classmethod
    def _extract_log_data(cls, line: str) -> str:
        try:
            data: dict = json.loads(line)
        except json.JSONDecodeError:
            return line
        return data.get("line")


class SellerLogReader:
    @classmethod
    def read(cls, filepaths: list[str] = None, file_dir_path: str = None) -> list[str]:
        if filepaths:
            for filepath in filepaths:
                data: bytes = FileSaveHelper.read(filepath=filepath)
                decoded = data.decode('utf-8')
                lines = decoded.splitlines()
                return lines
        return []

    @classmethod
    def parse(cls, lines: list[str]) -> list[DatadogLog]:
        res: list[DatadogLog] = []
        for line in lines[1:]:
            columns: list[str] = ReaderUtil.parse_csv_line_with_csv(line=line)
            data = cls._parse_csv_line(line=columns)
            if data:
                res.append(data)
        return sorted(res, key=lambda x: x.request_time)

    @classmethod
    def _parse_csv_line(cls, line: list[str]) -> DatadogLog | None:
        '''
        Date, 0
        ably_market_sno, 1
        @ably_sno, 2
        @prices, 3
        consumer_origin, 4
        @price_origin, 5
        @consumer_price_adjustment.discount_type, 6
        @consumer_price_adjustment.discount_rate, 7
        @consumer_price_adjustment.discount_price, 8
        @consumer_price_adjustment.ended_at, 9
        @consumer_price_adjustment.started_at, 10
        Message 11

        "2025-01-02T23:47:16.270Z","18675","""37364075""",,"43000","14900","0","0","28100",,,"update_seller_goods"
        "2025-01-08T02:09:39.868Z","32368","""21503547""","[{""app_type"":0,""discount_policy"":{""policy_type"":0,""policy_value"":111000},""consumer"":1800000},{""app_type"":1,""discount_policy"":{""policy_type"":0,""policy_value"":111000},""consumer"":1800000}]",,,,,,,,"update_seller_goods"
        '''
        dt: datetime = cls._parse_datetime(line[0]) + timedelta(hours=9)
        started_at: datetime = cls._parse_datetime(line[9], datetime(1970, 1, 1))
        ended_at: datetime = cls._parse_datetime(line[10], datetime(9999, 12, 31))
        prices: list[dict] = json.loads(line[3]) if line[3] else []
        if prices:
            app_price: dict = prices[0]
            discount_type: int = app_price.get("discount_policy", {}).get("policy_type", 0)
            app_consumer: int = app_price.get("consumer")
            discount_value: int = app_price.get("discount_policy", {}).get("policy_value", 0)
            discount_price: int = discard_ones_digit(discount_value)
            if discount_type == 1 and discount_value > 0:
                # 정률
                discount_price = app_consumer * discount_value

            started_at: datetime = app_price.get("discount_policy", {}).get("started_at", datetime(1970, 1, 1))
            ended_at: datetime = app_price.get("discount_policy", {}).get("ended_at", datetime(9999, 12, 31))
            return DatadogLog(
                market_sno=ReaderUtil.parse_int(line[1]),
                goods_sno=ReaderUtil.parse_int(line[2]),
                consumer_origin=app_consumer,
                price_origin=ReaderUtil.parse_int(line[5]),
                discount_type=0,
                discount_rate=0,
                discount_price=discount_price,
                discount_started_at=cls._parse_datetime(started_at, datetime(1970, 1, 1)),
                discount_ended_at=cls._parse_datetime(ended_at, datetime(9999, 12, 31)),
                request_time=dt,
            )
        else:
            return DatadogLog(
                market_sno=ReaderUtil.parse_int(line[1]),
                goods_sno=ReaderUtil.parse_int(line[2]),
                consumer_origin=ReaderUtil.parse_int(line[4]),
                price_origin=ReaderUtil.parse_int(line[5]),
                discount_type=ReaderUtil.parse_int(line[6]) if line[6] else -1,
                discount_rate=ReaderUtil.parse_int(line[7]) if line[7] else -1,
                discount_price=ReaderUtil.parse_int(line[8]) if line[8] else -1,
                discount_started_at=started_at,
                discount_ended_at=ended_at,
                request_time=dt,
            )

    @classmethod
    def _parse_datetime(cls, datetime_str: str | datetime, default_dt: datetime = None) -> datetime | None:
        '''
        Parses datetime strings in formats:
        - "2025-01-13 04:48:59.005"     # With milliseconds, space separator
        - "2025-01-13T04:48:59.005Z"    # With milliseconds, ISO format
        - "2025-01-03T06:50:06Z"        # Without milliseconds, ISO format
        '''
        if isinstance(datetime_str, datetime):
            return datetime_str

        cleaned_str = datetime_str.strip('"')
        if cleaned_str == "":
            return default_dt

        # Replace comma with period for proper datetime parsing
        cleaned_str = cleaned_str.replace(',', '.')

        formats_to_try = [
            "%Y-%m-%d %H:%M:%S.%f",  # With milliseconds, space separator
            "%Y-%m-%dT%H:%M:%S.%fZ",  # With milliseconds, ISO format
            "%Y-%m-%dT%H:%M:%SZ"  # Without milliseconds, ISO format
        ]

        for date_format in formats_to_try:
            try:
                return datetime.strptime(cleaned_str, date_format)
            except ValueError:
                continue

        return default_dt


class LogReader:
    HEADERS: list[str] = [
        "Date",  # 0
        "Host",  # 1
        "Service"  # 2
        "@ably_sno",  # 3
        "ably_market_sno",  # 4
        "consumer_origin",  # 5
        "@price_origin",  # 6
        "@consumer_price_adjustment.discount_type",  # 7
        "@consumer_price_adjustment.discount_rate",  # 8
        "@consumer_price_adjustment.discount_price",  # 9
        "@consumer_price_adjustment.started_at",  # 10
        "@consumer_price_adjustment.ended_at",  # 11
        "Message",  # 12
    ]

    @classmethod
    def read(cls, filepaths: list[str] = None, file_dir_path: str = None) -> list[str]:
        res: list[str] = []
        if filepaths:
            for filepath in filepaths:
                data: bytes = FileSaveHelper.read(filepath=filepath)
                decoded = data.decode('utf-8')
                lines = decoded.splitlines()

                # Skip header
                res.extend(lines[1:])
        elif file_dir_path:
            for filename in os.listdir(f"{os.getcwd()}/{file_dir_path}"):
                data: bytes = FileSaveHelper.read(filepath=f"{file_dir_path}{filename}")
                print(f"{file_dir_path}{filename}")
                decoded = data.decode('utf-8')
                lines = decoded.splitlines()
                res.extend(lines[1:])

        return res

    @classmethod
    def parse(cls, lines: list[str]) -> list[DatadogLog]:
        res: list[DatadogLog] = []
        for line in lines:
            columns: list[str] = ReaderUtil.parse_csv_line_with_csv(line=line)
            data = cls._parse_csv_line(line=columns)
            if data:
                res.append(data)
        return sorted(res, key=lambda x: x.request_time)

    @classmethod
    def _parse_csv_line(cls, line: list[str]) -> DatadogLog | None:
        '''
        33878,33056035,69000,62500,6500,,0,"2025-01-13T00:00:00Z","2999-12-31T23:59:59Z","2025-01-13 22:47:10,781",vendor_seller_update_goods
        {{.ably_market_sno}}, 0
        {{.ably_sno}}, 1
        {{.consumer_origin}}, 2
        {{.price_origin}}, 3
        {{.consumer_price_adjustment_discount_price}}, 4
        {{.consumer_price_adjustment_discount_rate}}, 5
        {{.consumer_price_adjustment_discount_type}}, 6
        "{{.consumer_price_adjustment_started_at}}", 7
        "{{.consumer_price_adjustment_ended_at}}", 8
        "{{.asctime}}", 9
        {{.message}} 10
        '''
        message = line[10]
        if message != "vendor_seller_update_goods":
            return None

        dt: datetime = cls._parse_datetime(line[9])
        started_at: datetime = cls._parse_datetime(line[7], datetime(1970, 1, 1))
        ended_at: datetime = cls._parse_datetime(line[8], datetime(9999, 12, 31))
        return DatadogLog(
            market_sno=ReaderUtil.parse_int(line[0]),
            goods_sno=ReaderUtil.parse_int(line[1]),
            consumer_origin=ReaderUtil.parse_int(line[2]),
            price_origin=ReaderUtil.parse_int(line[3]),
            discount_type=ReaderUtil.parse_int(line[6]) if line[6] else -1,
            discount_rate=ReaderUtil.parse_int(line[5]) if line[5] else -1,
            discount_price=ReaderUtil.parse_int(line[4]) if line[4] else -1,
            discount_started_at=started_at,
            discount_ended_at=ended_at,
            request_time=dt,
        )

    @classmethod
    def _parse_datetime(cls, datetime_str: str, default_dt: datetime = None) -> datetime | None:
        cleaned_str = datetime_str.strip('"')
        if cleaned_str == "":
            return default_dt

        # Replace comma with period for proper datetime parsing
        cleaned_str = cleaned_str.replace(',', '.')

        try:
            return datetime.strptime(cleaned_str, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            return default_dt


class ItemReader:
    @classmethod
    def read(cls, filepaths: list[str]) -> list[str]:
        res: list[str] = []
        for filepath in filepaths:
            print("filepath: ", filepath)
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
        goods_option_sno, 3
        goodsno, 4
        goodsnm, 5
        price, 6
        memberdc,
        emoney,
        coupon,
        ea, 10
        reserve,
        checked_at 12
        '''
        res: list[OrderItem] = []
        for line in lines:
            columns: list[str] = ReaderUtil.parse_csv_line_with_csv(line=line)
            checked_at: datetime = datetime.strptime(columns[12], "%Y-%m-%d %H:%M:%S.%f")
            res.append(
                OrderItem(
                    item_sno=ReaderUtil.parse_int(columns[0]),
                    order_sno=ReaderUtil.parse_int(columns[1]),
                    market_sno=int(columns[2]),
                    option_sno=int(columns[3]),
                    goods_sno=ReaderUtil.parse_int(columns[4]),
                    goods_name=columns[5],
                    price=ReaderUtil.parse_int(columns[6]),
                    quantity=ReaderUtil.parse_int(columns[10]),
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
            if goods_set and invalid.context.goods_sno not in goods_set:
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
            # 'goods_name',
            'goods_sno',
            'option_sno',
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
                # f'"{invalid.context.goods_name}"',  # Quote the name to handle commas
                str(invalid.context.goods_sno),
                str(invalid.item.option_sno),
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

    @classmethod
    def encode_bytes(cls, lines: list[str]) -> bytes:
        return '\n'.join(lines).encode('utf-8')


if __name__ == '__main__':
    print('program starts to parse..')
    # lines: list[str] = GrafanaLogReader.read(filepaths=['data/logs/o.jsonl'], limit=None)
    # lines: list[str] = GrafanaLogReader.read(filepaths=['data/logs/o.jsonl'], limit=None)
    # FileSaveHelper.save(data=DataPrinter.encode_bytes(lines=lines), filepath='data/parsed_logs.csv')
    # for line in lines:
    #     print('grafana-line: ', line)
    vendor_logs: list[DatadogLog] = LogReader.parse(lines=LogReader.read(file_dir_path="data/logs/"))
    seller_logs: list[DatadogLog] = SellerLogReader.parse(
        lines=SellerLogReader.read(filepaths=["data/seller_logs.csv"])
    )
    logs: list[DatadogLog] = sorted(vendor_logs + seller_logs, key=lambda x: x.request_time)
    items: list[OrderItem] = ItemReader.parse(lines=ItemReader.read(filepaths=['data/ably_gd_order_item.csv']))
    print('logs: ', len(logs))
    invalids: list[InvalidData] = ItemAnalyzer.analyze(logs=logs, items=items)

    print('len(vendor_logs): ', len(vendor_logs))
    print('len(items): ', len(items))
    print('len(invalids): ', len(invalids))
    print('unique goods: ', len(set([invalid.context.goods_sno for invalid in invalids])))
    FileSaveHelper.save(data=DataPrinter.map_csv(data=invalids), filepath='data/invalids_order_items.csv')
    # DataPrinter.print(data=invalids, goods_set=set())
