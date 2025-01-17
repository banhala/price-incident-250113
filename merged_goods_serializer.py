import msgpack

class MergedGoodsSerializer:
    @classmethod
    def serialize(cls, goods_sno: list[int]) -> bytes:
        return msgpack.packb(goods_sno)

    @classmethod
    def deserialize(cls, data: bytes) -> list[int]:
        return msgpack.unpackb(data)
