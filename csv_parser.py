import csv
from datetime import datetime
from io import StringIO

import pytz

from src.model.raw_csv import RawCsvGoodsOption, RawCsvPolicy, RawCsvDeal


class CsvParser:
    @classmethod
    def parse_raw_policy(cls, line: str) -> RawCsvPolicy:
        '''
        '''
        columns: list[str] = cls._parse_columns(line=line)
        is_active: bool = columns[0] == "true"
        status: int = int(columns[1])
        pricing_strategy: int = int(columns[2])
        policy_value: int = int(columns[3])
        policy_type: int = int(columns[4])
        market_sno: int = int(columns[5]) if columns[5] else 0
        goods_sno: int = int(columns[6]) if columns[6] else 0
        discount_method: int = int(columns[7])
        started_at: datetime = cls._parse_datetime_with_tz(columns[8])
        ended_at = cls._parse_datetime_with_tz(columns[9])
        operation_type: str = columns[10]
        transaction_time: str = columns[12]
        dt: str = columns[13]
        policy = RawCsvPolicy(
            is_active=is_active,
            status=status,
            pricing_strategy=pricing_strategy,
            policy_value=policy_value,
            policy_type=policy_type,
            market_sno=market_sno,
            goods_sno=goods_sno,
            discount_method=discount_method,
            started_at=started_at,
            ended_at=ended_at,
            operation_type=operation_type,
            transaction_time=cls._parse_timestamp(timestamp_str=transaction_time),
            dt=dt,
        )
        return policy

    @classmethod
    def parse_raw_deal(cls, line: str) -> RawCsvDeal:
        '''
        '''
        columns: list[str] = [raw_val[1:-1] for raw_val in line.split(',')]
        sno: int = int(columns[0])
        goods_sno: int = int(columns[1])
        goods_discount_policy_sno: int = int(columns[2])
        thumbnail_price: int = int(float(columns[3]))
        is_enabled: bool = columns[4] == "true"
        priority: int = int(columns[5])
        app_type: int = int(columns[6])
        started_at: datetime = cls._parse_datetime_with_tz(columns[7])
        ended_at: datetime = cls._parse_datetime_with_tz(columns[8])
        operation_type: str = columns[9]
        deleted: bool = columns[10] == "true"
        transaction_time: str = columns[11]
        dt: str = columns[12]
        deal = RawCsvDeal(
            sno=sno,
            goods_sno=goods_sno,
            goods_discount_policy_sno=goods_discount_policy_sno,
            thumbnail_price=thumbnail_price,
            is_enabled=is_enabled,
            priority=priority,
            app_type=app_type,
            started_at=started_at,
            ended_at=ended_at,
            operation_type=operation_type,
            deleted=deleted,
            transaction_time=cls._parse_timestamp(timestamp_str=transaction_time),
            dt=dt,
        )
        return deal

    @classmethod
    def parse_raw_option(cls, line: str) -> RawCsvGoodsOption:
        '''
        '''
        columns: list[str] = cls._parse_columns(line=line)
        market_sno: int = int(columns[0]) if columns[0] else 0
        goods_sno: int = int(columns[1]) if columns[1] else 0
        option_sno: int = int(columns[2])
        consumer_origin: int = int(columns[3])
        price_origin: int = int(columns[4])
        total_additional_price: int = int(columns[5])
        operation_type: str = columns[6]
        deleted: str = columns[7]
        transaction_time: str = columns[8]
        dt: str = columns[9]
        option = RawCsvGoodsOption(
            market_sno=market_sno,
            goods_sno=goods_sno,
            option_sno=option_sno,
            consumer_origin=consumer_origin,
            price_origin=price_origin,
            total_additional_price=total_additional_price,
            operation_type=operation_type,
            transaction_time=cls._parse_timestamp(timestamp_str=transaction_time),
            dt=dt,
        )
        return option

    @classmethod
    def _parse_timestamp(cls, timestamp_str: str) -> datetime:
        """
        Parse a timestamp string in format '2025-01-07 00:00:01.000 Asia/Seoul' to datetime
        """
        if not timestamp_str:
            return datetime(1970, 1, 1)

        try:
            dt_string, tz_string = timestamp_str.rsplit(" ", 1)
            dt = datetime.strptime(dt_string, "%Y-%m-%d %H:%M:%S.%f")
            timezone = pytz.timezone(tz_string)
            dt_with_tz = timezone.localize(dt)
            return datetime(
                year=dt_with_tz.year,
                month=dt_with_tz.month,
                day=dt_with_tz.day,
                hour=dt_with_tz.hour,
                minute=dt_with_tz.minute,
                second=dt_with_tz.second,
                microsecond=dt_with_tz.microsecond,
            )

        except ValueError as e:
            raise ValueError(f"Failed to parse timestamp: {e}")

    @classmethod
    def _parse_datetime_with_tz(cls, dt_string: str) -> datetime:
        """
        Parse a datetime string in the format '9999-12-31 23:59:59.000 Asia/Seoul'
        """
        if not dt_string:
            return datetime(1970, 1, 1)

        dt_part, _ = dt_string.rsplit(' ', 1)
        dt = datetime.strptime(dt_part, '%Y-%m-%d %H:%M:%S.%f')
        return dt

    @classmethod
    def _parse_columns(cls, line: str) -> list[str]:
        return cls.parse_csv_line_with_csv(line=line)

    @classmethod
    def parse_csv_line_with_csv(cls, line: str) -> list[str]:
        return next(csv.reader(StringIO(line)))
