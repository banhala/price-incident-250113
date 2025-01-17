import json
from collections import defaultdict
from datetime import datetime
from itertools import islice

from create_revision import CreateRevisionService
from merged_goods_serializer import MergedGoodsSerializer
from platform_consumer_serializer import PlatformConsumerSerializer
from adj_serializer import AdjSerializer
from csv_parser import CsvParser
from csv_reader import CsvReader
from deal_serializer import DealSerializer
from file_save_helper import FileSaveHelper
from option_serializer import OptionSerializer
from policy_serializer import PolicySerializer
from prepare_revision import PrepareRevisionService
from revision_serializer import RevisionSerializer
from src.model.Revision import Revision
from src.model.edit_revision import EditRevision
from src.model.prepared_data import PreparedData
from src.model.raw_csv import RawCsvGoodsOption, RawCsvPolicy, RawCsvDeal, RawCsvPlatformConsumer, RawCsvAdj

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

print('program starts..')


def process_goods(limit: int | None) -> None:
    goods_list: list[int] = [
        CsvParser.parse_raw_goods(line=line) for line in
        CsvReader.read(filepath='data/goods.csv', limit=limit)
    ]
    pass


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
    serialized_data: bytes = OptionSerializer.serialize(raw_map=option_map)
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
        CsvReader.read(filepath='data/deals.csv', limit=limit)
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


def process_platform_consumers(limit: int | None) -> None:
    # 1. parse the csv file
    platform_consumers: list[RawCsvPlatformConsumer] = [
        CsvParser.parse_raw_platform_consumer(line=line) for line in
        CsvReader.read(filepath='data/consumer_250107_250113.csv', limit=limit)
    ]
    consumer_map: dict[int, list[RawCsvPlatformConsumer]] = defaultdict(list)
    for obj in platform_consumers:
        consumer_map[obj.goods_sno].append(obj)

    # 3. save
    serialized_data: bytes = PlatformConsumerSerializer.serialize(raw_map=consumer_map)
    FileSaveHelper.save(data=serialized_data, filepath='data/processed/consumer_map.msgpack')

    serialized_data: bytes = FileSaveHelper.read(filepath='data/processed/consumer_map.msgpack')
    deserialized_map: dict[int, list[RawCsvPlatformConsumer]] = PlatformConsumerSerializer.deserialize(
        data=serialized_data)
    for k, v in deserialized_map.items():
        for plat in v:
            print('plat: ', plat)


def process_adjs(limit: int | None) -> None:
    # 1. parse the csv file
    adjs: list[RawCsvAdj] = [
        CsvParser.parse_raw_adj(line=line) for line in
        CsvReader.read(filepath='data/adj_250107_250113.csv', limit=limit)
    ]
    adj_map: dict[int, list[RawCsvAdj]] = defaultdict(list)
    for obj in adjs:
        adj_map[obj.goods_sno].append(obj)

    # 3. save
    serialized_data: bytes = AdjSerializer.serialize(raw_map=adj_map)
    FileSaveHelper.save(data=serialized_data, filepath='data/processed/adj_map.msgpack')

    # serialized_data: bytes = FileSaveHelper.read(filepath='data/processed/adj_map.msgpack')
    # deserialized_map: dict[int, list[RawCsvAdj]] = AdjSerializer.deserialize(data=serialized_data)
    # for k, v in deserialized_map.items():
    #     for adj in v:
    #         print('adj: ', adj)


LIMIT: int | None = None
begin_time: datetime = datetime.now()
# process_options(limit=LIMIT)
# process_policies(limit=LIMIT)
# process_deals(limit=LIMIT)
# process_adjs(limit=LIMIT)
# process_platform_consumers(limit=LIMIT)

# goods_sno_list: list[int] = [
#     CsvParser.parse_raw_goods(line=line) for line in
#     CsvReader.read(filepath='data/goods.csv', limit=LIMIT)
# ]
# print('goods_list length: ', len(goods_list))

# serialized_data: bytes = FileSaveHelper.read(filepath='data/processed/consumer_map.msgpack')
# deserialized_map: dict[int, list[RawCsvPlatformConsumer]] = PlatformConsumerSerializer.deserialize(data=serialized_data)



# data: PreparedData = PrepareRevisionService.prepare(
#     goods_sno_list=goods_sno_list,
#     deal_filepath='data/processed/deal_map.msgpack',
#     option_filepath='data/processed/option_map.msgpack',
#     consumer_filepath='data/processed/consumer_map.msgpack',
#     adj_filepath='data/processed/adj_map.msgpack',
# )
#
# merged_goods_set: set[int] = (
#         set(goods_sno_list)
#         & set(data.deal_map.keys())
#         & set(data.option_map.keys())
#         & set(data.platform_consumer_map.keys())
#         & set(data.adj_map.keys())
# )
# print('merged_goods_set: ', len(merged_goods_set))
# print('saving merged_goods_set..')
# FileSaveHelper.save(
#     data=MergedGoodsSerializer.serialize(goods_sno=list(merged_goods_set)),
#     filepath='data/processed/merged_goods.msgpack',
# )
#
# print('saving adj_map..')
# FileSaveHelper.save(
#     data=AdjSerializer.serialize(raw_map=data.adj_map),
#     filepath='data/processed/draft/adj_map.msgpack',
# )
#
# print('saving consumer_map..')
# FileSaveHelper.save(
#     data=PlatformConsumerSerializer.serialize(raw_map=data.platform_consumer_map),
#     filepath='data/processed/draft/consumer_map.msgpack',
# )
#
# print('saving deal_map..')
# FileSaveHelper.save(
#     data=DealSerializer.serialize(raw_map=data.deal_map),
#     filepath='data/processed/draft/deal_map.msgpack'
# )
#
# print('saving option_map..')
# FileSaveHelper.save(
#     data=OptionSerializer.serialize(raw_map=data.option_map),
#     filepath='data/processed/draft/option_map.msgpack',
# )

# serialized_data: bytes = FileSaveHelper.read(filepath='data/processed/option_map.msgpack')
# option_map: dict[int, list[RawCsvGoodsOption]] = OptionSerializer.deserialize_option(data=serialized_data)
# print('option_map: ', len(option_map))

# serialized_data: bytes = FileSaveHelper.read(filepath='data/processed/deal_map.msgpack')
# deal_map: dict[int, list[RawCsvDeal]] = DealSerializer.deserialize(data=serialized_data)
# deal_map: dict[int, list[RawCsvDeal]] = dict_slice(deal_map, start=0, stop=10)
# selected_option_map: dict[int, list[RawCsvGoodsOption]] = {}
# goods_sno_list: list[int] = list(deal_map.keys())
# for goods_sno in goods_sno_list:
#     selected_option_map[goods_sno] = option_map[goods_sno]

# goods_sno_list: list[int] = MergedGoodsSerializer.deserialize(
#     data=FileSaveHelper.read(filepath='data/processed/merged_goods.msgpack')
# )
# data: PreparedData = PrepareRevisionService.prepare(
#     goods_sno_list=goods_sno_list,
#     deal_filepath='data/processed/deal_map.msgpack',
#     option_filepath='data/processed/option_map.msgpack',
#     consumer_filepath='data/processed/consumer_map.msgpack',
#     adj_filepath='data/processed/adj_map.msgpack',
# )
# revisions: list[EditRevision] = CreateRevisionService.create_revision2(data=data)
# revision_bytes: bytes = RevisionSerializer.serialize(revisions=revisions)
# FileSaveHelper.save(data=revision_bytes, filepath='data/out/revision.csv')
#
# print('revisions: ', revisions)
# print('revisions len: ', len(revisions))
# print('revisions goods_sno: ', [r.goods_sno for r in revisions])
# print('revisions goods_sno len: ', len(set([r.goods_sno for r in revisions])))

revision_bytes: bytes = FileSaveHelper.read(filepath='data/out/revision.csv')
revisions: list[Revision] = RevisionSerializer.deserialize(data=revision_bytes)
revision_goods_sno_list: list[int] = [r.goods_sno for r in revisions]
json_bytes: bytes = json.dumps(revision_goods_sno_list, ensure_ascii=False).encode('utf-8')
FileSaveHelper.save(
    data=json_bytes,
    filepath='data/out/revision_goods_sno_list.json',
)
print('revisions: ', [r.goods_sno for r in revisions])

print('revisions len: ', len(revisions))
print('revisions goods_sno: ', [r.goods_sno for r in revisions])
print('revisions goods_sno len: ', len(set([r.goods_sno for r in revisions])))

end_time: datetime = datetime.now()
print('elapsed time: ', end_time - begin_time)
