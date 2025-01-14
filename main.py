from collections import defaultdict
from datetime import datetime

from csv_parser import CsvParser
from csv_reader import CsvReader
from deal_serializer import DealSerializer
from file_save_helper import FileSaveHelper
from option_serializer import OptionSerializer
from policy_serializer import PolicySerializer
from src.model.raw_csv import RawCsvGoodsOption, RawCsvPolicy, RawCsvDeal

'''
{option_sno: [(deal, option_binlog)]}

option_sno/deal.txt
option_sno/option.txt


output:
"option_sno, correct_price, wrong_price, consumer_origin, price_origin, thumbnail_price, deal_sno, policy_sno, is_enabled, started_at, ended_at, dt, transaction_time"

1. read csv file
2. form the columns
3. group by {goods_option_sno: Rev(consumer_origin, price_origin)}
'''


def process_options(limit: int | None) -> None:
    # 1. parse the csv file
    options: list[RawCsvGoodsOption] = [
        CsvParser.parse_raw_option(line=line) for line in
        CsvReader.read(filepath='data/options_250107_250113.csv', limit=limit)
    ]

    # 2. group by goods_sno
    option_map: dict[int, list[RawCsvGoodsOption]] = defaultdict(list)
    for opt in options:
        option_map[opt.goods_sno].append(opt)

    # 3. save
    serialized_data: bytes = OptionSerializer.serialize_option(raw_map=option_map)
    FileSaveHelper.save(data=serialized_data, filepath='data/processed/option_map.msgpack')

    # serialized_data: bytes = FileSaveHelper.read(filepath='data/processed/option_map.msgpack')
    # deserialized_map: dict[int, list[RawCsvGoodsOption]] = OptionSerializer.deserialize_option(data=serialized_data)
    # print('deserialized_map: ', len(deserialized_map))


def process_policies(limit: int | None) -> None:
    # 1. parse the csv file
    policies: list[RawCsvPolicy] = [
        CsvParser.parse_raw_policy(line=line) for line in
        CsvReader.read(filepath='data/policies_250107_250113.csv', limit=limit)
    ]
    policy_map: dict[int, list[RawCsvPolicy]] = defaultdict(list)
    for policy in policies:
        policy_map[policy.goods_sno].append(policy)

    # 3. save
    serialized_data: bytes = PolicySerializer.serialize(raw_map=policy_map)
    FileSaveHelper.save(data=serialized_data, filepath='data/processed/policy_map.msgpack')

    # serialized_data: bytes = FileSaveHelper.read(filepath='data/processed/policy_map.msgpack')
    # deserialized_map: dict[int, list[RawCsvPolicy]] = PolicySerializer.deserialize(data=serialized_data)
    # for k, v in deserialized_map.items():
    #     for policy in v:
    #         print('policy ', policy)


def process_deals(limit: int | None) -> None:
    # 1. parse the csv file
    deals: list[RawCsvDeal] = [
        CsvParser.parse_raw_deal(line=line) for line in
        CsvReader.read(filepath='data/deals_250107_250113.csv', limit=limit)
    ]
    deal_map: dict[int, list[RawCsvDeal]] = defaultdict(list)
    for deal in deals:
        deal_map[deal.goods_sno].append(deal)

    # 3. save
    serialized_data: bytes = DealSerializer.serialize(raw_map=deal_map)
    FileSaveHelper.save(data=serialized_data, filepath='data/processed/deal_map.msgpack')

    # serialized_data: bytes = FileSaveHelper.read(filepath='data/processed/deal_map.msgpack')
    # deserialized_map: dict[int, list[RawCsvPolicy]] = DealSerializer.deserialize(data=serialized_data)
    # for k, v in deserialized_map.items():
    #     for policy in v:
    #         print('policy ', policy)


LIMIT: int | None = None
begin_time: datetime = datetime.now()

# process_options(limit=LIMIT)
# process_policies(limit=LIMIT)
process_deals(limit=LIMIT)
#
# deals: list[RawCsvDeal] = [
#     CsvParser.parse_raw_deal(line=line) for line in
#     CsvReader.read(filepath='data/deals_250107_250113.csv', limit=LIMIT)
# ]
end_time: datetime = datetime.now()
print('elapsed time: ', end_time - begin_time)
# print('policies: ', (policies))
# print('deals: ', (deals))
