from datetime import datetime

from csv_parser import CsvParser
from csv_reader import CsvReader
from src.model.edit_revision import EditRevision
from src.model.raw_csv import RawCsvGoodsOption, RawCsvPolicy, RawCsvDeal

rev = EditRevision(goods_sno=1, market_sno=2)
print('rev: ', rev)

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

LIMIT: int | None = 10
begin_time: datetime = datetime.now()
options: list[RawCsvGoodsOption] = [
    CsvParser.parse_raw_option(line=line) for line in
    CsvReader.read(filepath='data/options_250107_250113.csv', limit=LIMIT)
]

policies: list[RawCsvPolicy] = [
    CsvParser.parse_raw_policy(line=line) for line in
    CsvReader.read(filepath='data/policies_250107_250113.csv', limit=LIMIT)
]

deals: list[RawCsvDeal] = [
    CsvParser.parse_raw_deal(line=line) for line in
    CsvReader.read(filepath='data/deals_250107_250113.csv', limit=LIMIT)
]
end_time: datetime = datetime.now()
print('elapsed time: ', end_time - begin_time)

print('options: ', (options))
print('policies: ', (policies))
print('deals: ', (deals))


