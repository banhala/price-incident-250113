from src.model.Revision import Revision
from src.model.edit_revision import EditRevision
from datetime import datetime

class RevisionSerializer:
    HEADERS = [
        'goods_sno',
        'deal_sno',
        'goods_discount_policy_sno',
        'thumbnail_price',
        'consumer_origin',
        'price_origin',
        'platform_consumer',
        'discount_price',
        'discount_type',
        'is_wrong_price',
        'transaction_time'
    ]

    @classmethod
    def serialize(cls, revisions: list[EditRevision]) -> bytes:
        '''
        Serializes list of EditRevision objects into CSV format as bytes.
        '''
        data: list[str] = [','.join(cls.HEADERS)]  # Add headers as first row
        data.extend(cls._map_to_str(rev) for rev in revisions)
        return '\n'.join(data).encode('utf-8')

    @classmethod
    def deserialize(cls, data: bytes) -> list[Revision]:
        decoded = data.decode('utf-8')
        lines: list[str] = decoded.splitlines()
        
        if len(lines) <= 1:  # If only header or empty
            return []
            
        revisions = []
        for line in lines[1:]:  # Skip header
            values = line.split(',')
            revision = Revision(
                goods_sno=int(values[0]),
                deal_sno=int(values[1]),
                goods_discount_policy_sno=int(values[2]),
                thumbnail_price=int(values[3]),
                consumer_origin=int(values[4]),
                price_origin=int(values[5]),
                platform_consumer=int(values[6]),
                discount_price=int(values[7]),
                discount_type=int(values[8]),
                is_wrong_price=(values[9].lower() == 'true'),
                transaction_time=datetime.fromisoformat(values[10])
            )
            revisions.append(revision)
            
        return revisions

    @classmethod
    def _map_to_str(cls, rev: EditRevision) -> str:
        data = [
            rev.goods_sno,
            rev.deal.sno,
            rev.deal.goods_discount_policy_sno,
            rev.context.thumbnail_price,
            rev.context.consumer_origin,
            rev.context.price_origin,
            rev.context.platform_consumer,
            rev.context.discount_price,
            rev.context.discount_type,
            rev.context.thumbnail_price < (rev.context.platform_consumer - rev.context.discount_price),
            rev.transaction_time,
        ]
        return ','.join([str(x) for x in data])
