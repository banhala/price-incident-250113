# Preparation

CSV files required.

* gd_goods_20250107_20250113.csv
* gd_goods_option_250107_250113.csv
* goods_discount_policies_250107_250113.csv
* goods_deal_price_250107_250113.csv

# Calculation

The goal is to generate the following columns.

* market_sno
* goods_sno
* thumbnail_price
* price_origin


## Algorithm

### Input

With the following fields: 

* gd_goods: provides [vendor_id]
* gd_goods_option: provides [price_origin]
* goods_discount_policies: provides [started_at, ended_at, is_active, policy_type, policy_value, market_sno]
* goods_deal_price: provides [thumbnail_price]

### Step

For price_origin,

1. filter goods_sno where vendor_id > 0
2. take price_origin from gd_goods_option where goods_sno == gd_goods.goods_sno

For thumbnail_price,

1. filter goods_sno where vendor_id > 0
2. filter policies where started_at < now < ended_at && ac_active && market_sno != null
3. take thumbnail_price from goods_deal_price where goods_sno == gd_goods.goods_sno && is_enabled

### Output

1. goods.market_sno == deal.policy.market_sno -> [deal]
2. option.price_origin != deal.thumbnail_price -> [피해금액]
3. deal.thumbnail_price == order_item.price -> [보상아이템]