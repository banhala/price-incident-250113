from adj_serializer import AdjSerializer
from deal_serializer import DealSerializer
from file_save_helper import FileSaveHelper
from option_serializer import OptionSerializer
from platform_consumer_serializer import PlatformConsumerSerializer
from src.model.prepared_data import PreparedData
from src.model.raw_csv import RawCsvDeal, RawCsvGoodsOption, RawCsvPlatformConsumer, RawCsvAdj


class PrepareRevisionService:
    @classmethod
    def prepare(
            cls,
            goods_sno_list: list[int],
            deal_filepath: str,
            option_filepath: str,
            consumer_filepath: str,
            adj_filepath: str,
    ) -> PreparedData:
        return PreparedData(
            deal_map=cls._fetch_deals(
                filepath=deal_filepath,
                goods_sno_list=goods_sno_list,
            ),
            option_map=cls._fetch_options(
                filepath=option_filepath,
                goods_sno_list=goods_sno_list,
            ),
            platform_consumer_map=cls._fetch_platform_consumers(
                filepath=consumer_filepath,
                goods_sno_list=goods_sno_list,
            ),
            adj_map=cls._fetch_adjs(
                filepath=adj_filepath,
                goods_sno_list=goods_sno_list,
            ),
        )

    @classmethod
    def _fetch_options(cls, filepath: str, goods_sno_list: list[int]) -> dict[int, list[RawCsvGoodsOption]]:
        serialized_data: bytes = FileSaveHelper.read(filepath=filepath)
        option_map: dict[int, list[RawCsvGoodsOption]] = OptionSerializer.deserialize(data=serialized_data)
        if goods_sno_list:
            selected_option_map: dict[int, list[RawCsvGoodsOption]] = dict()
            for goods_sno in goods_sno_list:
                if goods_sno not in option_map:
                    print('option skipped goods_sno: ', goods_sno)
                    continue

                selected_option_map[goods_sno] = option_map[goods_sno]
            return selected_option_map
        else:
            return option_map

    @classmethod
    def _fetch_deals(cls, filepath: str, goods_sno_list: list[int]) -> dict[int, list[RawCsvDeal]]:
        serialized_data: bytes = FileSaveHelper.read(filepath=filepath)
        deal_map: dict[int, list[RawCsvDeal]] = DealSerializer.deserialize(data=serialized_data)
        skipped_cnt: int = 0
        if goods_sno_list:
            selected_deal_map: dict[int, list[RawCsvDeal]] = dict()
            for goods_sno in goods_sno_list:
                if goods_sno not in deal_map:
                    skipped_cnt += 1
                    continue

                selected_deal_map[goods_sno] = deal_map[goods_sno]
            print('deal skipped goods_sno: ', skipped_cnt)
            return selected_deal_map
        else:
            return deal_map

    @classmethod
    def _fetch_platform_consumers(
            cls,
            filepath: str,
            goods_sno_list: list[int],
    ) -> dict[int, list[RawCsvPlatformConsumer]]:
        data: bytes = FileSaveHelper.read(filepath=filepath)
        consumer_map: dict[int, list[RawCsvPlatformConsumer]] = PlatformConsumerSerializer.deserialize(data=data)
        if goods_sno_list:
            selected_map: dict[int, list[RawCsvPlatformConsumer]] = dict()
            for goods_sno in goods_sno_list:
                if goods_sno not in consumer_map:
                    print('consumer skipped goods_sno: ', goods_sno)
                    continue

                selected_map[goods_sno] = consumer_map[goods_sno]
            return selected_map
        else:
            return consumer_map

    @classmethod
    def _fetch_adjs(cls, filepath: str, goods_sno_list: list[int]) -> dict[int, list[RawCsvAdj]]:
        data: bytes = FileSaveHelper.read(filepath=filepath)
        adj_map: dict[int, list[RawCsvAdj]] = AdjSerializer.deserialize(data=data)
        if goods_sno_list:
            selected_map: dict[int, list[RawCsvAdj]] = dict()
            for goods_sno in goods_sno_list:
                if goods_sno not in adj_map:
                    print('adj skipped goods_sno: ', goods_sno)
                    continue

                selected_map[goods_sno] = adj_map[goods_sno]
            return selected_map
        else:
            return adj_map
