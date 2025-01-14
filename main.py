from csv_parser import CsvParser
from csv_reader import CsvReader
from src.model.edit_revision import EditRevision
from src.model.raw_csv import RawCsvGoodsOption, RawCsvPolicy, RawCsvDeal

rev = EditRevision(goods_sno=1, market_sno=2)
print('rev: ', rev)

'''
1. read csv file
2. form the columns
3. group by {goods_option_sno: Rev(consumer_origin, price_origin)}
'''

options: list[RawCsvGoodsOption] = [
    CsvParser.parse_raw_option(line=line) for line in
    CsvReader.read(filepath='data/options_250107_250113.csv', limit=10)
]

policies: list[RawCsvPolicy] = [
    CsvParser.parse_raw_policy(line=line) for line in
    CsvReader.read(filepath='data/policies_250107_250113.csv', limit=10)
]

deals: list[RawCsvDeal] = [
    CsvParser.parse_raw_deal(line=line) for line in
    CsvReader.read(filepath='data/deals_250107_250113.csv', limit=10)
]

print('options: ', options)
print('policies: ', policies)
print('deals: ', deals)
