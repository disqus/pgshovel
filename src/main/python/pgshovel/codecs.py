import json as stdjson
import uuid

from pgshovel.events import (
    MutationBatch,
    MutationEvent,
)


class JsonCodec(object):
    def __default(self, value):
        if isinstance(value, MutationBatch):
            return {
                'id': value.id,
                'start_txid': value.start_txid,
                'end_txid': value.end_txid,
                'node': value.node,
                'events': value.events,
            }
        elif isinstance(value, MutationEvent):
            return {
                'id': value.id,
                'schema': value.schema,
                'table': value.table,
                'operation': value.operation,
                'primary_key_columns': value.primary_key_columns,
                'states': value.states,
                'transaction_id': value.transaction_id,
                'timestamp': value.timestamp,
            }
        elif isinstance(value, uuid.UUID):
            return value.hex
        else:
            raise TypeError('%r is not JSON serializable' % (value,))

    def encode(self, value):
        return stdjson.dumps(value, default=self.__default)


json = JsonCodec()
