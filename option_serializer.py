import dataclasses
from datetime import datetime

import msgpack

from src.model.raw_csv import RawCsvGoodsOption


class OptionSerializer:
    @classmethod
    def serialize_option(cls, raw_map: dict[int, list[dataclasses.dataclass]]) -> bytes:
        converted = {
            k: [
                dataclasses.asdict(opt) | {'transaction_time': opt.transaction_time.isoformat()}
                for opt in v
            ]
            for k, v in raw_map.items()
        }
        return msgpack.packb(converted)

    @classmethod
    def deserialize_option(cls, data: bytes) -> dict[int, list[dataclasses.dataclass]]:
        # Either use strict_map_key=False
        data = msgpack.unpackb(data, strict_map_key=False)
        # OR keep the keys as strings and convert back to int when processing
        result = {}
        for key, options in data.items():
            # Convert string key back to int
            result[int(key)] = [
                RawCsvGoodsOption(
                    **{**opt, 'transaction_time': datetime.fromisoformat(opt['transaction_time'])}
                )
                for opt in options
            ]
        return result
