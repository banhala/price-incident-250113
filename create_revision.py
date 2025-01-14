from copy import deepcopy
from datetime import datetime

from src.model.edit_revision import EditRevision
from src.model.option_context import OptionContext
from src.model.raw_csv import RawCsvDeal, RawCsvGoodsOption


class CreateRevisionService:
    @classmethod
    def create_revision(
            cls,
            deal_map: dict[int, list[RawCsvDeal]],
            option_map: dict[int, list[RawCsvGoodsOption]],
    ) -> list[EditRevision]:
        print('deals: ', len(deal_map))
        print('options: ', len(option_map))
        res: list[EditRevision] = []
        for goods_sno, deals in deal_map.items():
            ctx: OptionContext = OptionContext()
            deals: list[RawCsvDeal] = sorted(deals, key=lambda x: x.transaction_time)
            opts: list[RawCsvGoodsOption] = sorted(option_map[goods_sno], key=lambda x: x.transaction_time)
            for deal in deals:
                cls._apply_revision(context=ctx, options=opts, changed_at=deal.transaction_time)
                if ctx.price_origin != deal.thumbnail_price:
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
    def _apply_revision(cls, context: OptionContext, options: list[RawCsvGoodsOption], changed_at: datetime) -> None:
        while options and options[0].transaction_time <= changed_at:
            opt: RawCsvGoodsOption = options.pop(0)
            if opt.operation_type == 'c':
                context.created_at = opt.transaction_time
            elif opt.operation_type == 'u':
                context.updated_at = opt.transaction_time
            elif opt.operation_type == 'd':
                context.goods_sno = -1

            context.goods_sno = opt.goods_sno
            context.option_sno = opt.option_sno
            context.consumer_origin = opt.consumer_origin
            context.price_origin = opt.price_origin
            context.total_additional_price = opt.total_additional_price
