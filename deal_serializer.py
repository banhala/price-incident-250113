import dataclasses
from datetime import datetime

import msgpack

from src.model.raw_csv import RawCsvDeal


class DealSerializer:
    @classmethod
    def serialize(cls, raw_map: dict[int, list[dataclasses.dataclass]]) -> bytes:
        converted = {
            k: [
                dataclasses.asdict(opt) | {
                    'transaction_time': opt.transaction_time.isoformat(),
                    'started_at': opt.started_at.isoformat(),
                    'ended_at': opt.ended_at.isoformat(),
                }
                for opt in v
            ]
            for k, v in raw_map.items()
        }
        return msgpack.packb(converted)

    @classmethod
    def deserialize(cls, data: bytes) -> dict[int, list[dataclasses.dataclass]]:
        # Either use strict_map_key=False
        data = msgpack.unpackb(data, strict_map_key=False)
        # OR keep the keys as strings and convert back to int when processing
        result = {}
        for key, options in data.items():
            # Convert string key back to int
            result[int(key)] = [
                RawCsvDeal(
                    **{
                        **opt,
                        'transaction_time': datetime.fromisoformat(opt['transaction_time']),
                        'started_at': datetime.fromisoformat(opt['started_at']),
                        'ended_at': datetime.fromisoformat(opt['ended_at']),
                    }
                )
                for opt in options
            ]
        return result
