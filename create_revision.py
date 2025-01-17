from copy import deepcopy
from datetime import datetime

from prepare_revision import PreparedData
from src.model.edit_revision import EditRevision
from src.model.option_context import GoodsContext
from src.model.raw_csv import RawCsvDeal, RawCsvGoodsOption, RawCsvPlatformConsumer, RawCsvAdj
from src.util import discard_ones_digit


class CreateRevisionService:
    @classmethod
    def create_revision2(cls, data: PreparedData) -> list[EditRevision]:
        res: list[EditRevision] = []
        for goods_sno, deals in data.deal_map.items():
            ctx: GoodsContext = GoodsContext(goods_sno=goods_sno)
            deals: list[RawCsvDeal] = sorted(deals, key=lambda x: x.transaction_time)
            while deals:
                deal: RawCsvDeal = deals.pop(0)
                cls._apply_revision2(context=ctx, data=data, deal=deal, changed_at=deal.transaction_time)
                # 계산

                wrong_price: int = ctx.thumbnail_price
                correct_price1: int = discard_ones_digit(ctx.platform_consumer - ctx.discount_price)
                correct_price2: int = discard_ones_digit(
                        max(ctx.consumer_origin, ctx.price_origin) - abs(ctx.consumer_origin - ctx.price_origin)
                )

                if wrong_price == correct_price1 or wrong_price == correct_price2:
                    continue

                res.append(
                    EditRevision(
                        goods_sno=goods_sno,
                        context=deepcopy(ctx),
                        deal=deepcopy(deal),
                        transaction_time=deal.transaction_time,
                    )
                )
        return res

    @classmethod
    def _apply_revision2(
            cls,
            context: GoodsContext,
            data: PreparedData,
            deal: RawCsvDeal,
            changed_at: datetime,
    ) -> None:
        if deal.is_enabled and (deal.operation_type == 'c' or deal.operation_type == 'u'):
            context.thumbnail_price = deal.thumbnail_price
        else:
            context.goods_sno = -1

        if context.goods_sno in data.option_map:
            options = sorted(data.option_map[context.goods_sno], key=lambda x: x.transaction_time)
            while options and options[0].transaction_time <= changed_at:
                opt: RawCsvGoodsOption = options.pop(0)
                if opt.operation_type == 'c' or opt.operation_type == 'u':
                    context.price_origin = opt.price_origin
                    context.consumer_origin = opt.consumer_origin
                    context.total_additional_price = opt.total_additional_price

                elif opt.operation_type == 'd':
                    context.goods_sno = -1

        if context.goods_sno in data.platform_consumer_map:
            platform_consumers = sorted(data.platform_consumer_map[context.goods_sno], key=lambda x: x.transaction_time)
            while platform_consumers and platform_consumers[0].transaction_time <= changed_at:
                pc: RawCsvPlatformConsumer = platform_consumers.pop(0)
                if pc.operation_type == 'c' or pc.operation_type == 'u':
                    context.platform_consumer = pc.consumer_origin
                    context.platform_total_additional_price = pc.total_additional_price

                elif pc.operation_type == 'd':
                    context.goods_sno = -1

        if context.goods_sno in data.adj_map:
            adjs = sorted(data.adj_map[context.goods_sno], key=lambda x: x.transaction_time)
            while adjs and adjs[0].transaction_time <= changed_at:
                adj: RawCsvAdj = adjs.pop(0)
                if adj.operation_type == 'c' or adj.operation_type == 'u':
                    context.discount_type = adj.discount_type
                    context.discount_price = adj.discount_price
                    context.started_at = adj.started_at
                    context.ended_at = adj.ended_at

                elif adj.operation_type == 'd':
                    context.goods_sno = -1
