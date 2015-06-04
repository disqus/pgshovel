"""
.. warning::

    Most JSON implementations treat numbers (including integers) as the
    equivalent ECMA numeric type: a double-precision 64-bit binary format IEEE
    754 value. (Ref: http://www.ecma-international.org/ecma-262/5.1/#sec-4.3.19)

    This is handled in encoded ``MutationBatch`` and ``MutationEvent`` types by
    converting integer fields to their string representations, but is not
    automatically handled for trigger data. If your dataset contains large
    integer types (and precision is important), it is recommended that you use
    an alternate codec that doesn't suffer from this issue (such as the codec
    located in ``pgshovel.contrib.msgpack``.)

"""
import json as stdjson
import uuid

from pgshovel.events import (
    MutationBatch,
    MutationEvent,
)


class JsonCodec(object):
    def __init__(self, extensions={}, include_type_annotations=True):
        self.__extensions_by_code = {}
        self.__extensions_by_type = {}
        for code, (type, encoder, decoder) in extensions.items():
            assert type not in self.__extensions_by_type, 'duplicate type registration: %s' % (type,)
            self.__extensions_by_code[code] = decoder
            self.__extensions_by_type[type] = code, encoder

        self.__include_type_annotations = include_type_annotations

    def __default(self, value):
        registration = self.__extensions_by_type.get(type(value))
        if registration is None:
            return value

        code, encoder = registration
        if self.__include_type_annotations:
            return {
                '__type__': code,
                '__data__': encoder(self, value),
            }
        else:
            return encoder(self, value)

    def encode(self, value):
        return stdjson.dumps(value, default=self.__default)

    def __object_hook(self, data):
        code = data.get('__type__')
        if code is None:
            return data

        decoder = self.__extensions_by_code.get(code)
        if decoder is None:
            raise TypeError('unregistered extension type: %s' % (code,))

        return decoder(self, data['__data__'])

    def decode(self, payload):
        return stdjson.loads(payload, object_hook=self.__object_hook)


def encode_batch(codec, batch):
    return {
        'id': str(batch.id),
        'start_txid': str(batch.start_txid),
        'end_txid': str(batch.end_txid),
        'node': batch.node,
        'events': batch.events,
    }


def decode_batch(codec, data):
    data.update({
        'id': int(data['id']),
        'start_txid': int(data['start_txid']),
        'end_txid': int(data['end_txid']),
    })
    return MutationBatch(**data)


def encode_event(codec, event):
    return {
        'id': str(event.id),
        'schema': event.schema,
        'table': event.table,
        'operation': event.operation,
        'primary_key_columns': event.primary_key_columns,
        'states': event.states,
        'transaction_id': str(event.transaction_id),
        'timestamp': event.timestamp,
    }


def decode_event(codec, data):
    data.update({
        'id': int(data['id']),
        'transaction_id': int(data['transaction_id']),
    })
    return MutationEvent(**data)


def encode_uuid(codec, uuid):
    return uuid.hex


def decode_uuid(codec, data):
    return uuid.UUID(data)


extensions = {
    'MutationBatch': (MutationBatch, encode_batch, decode_batch,),
    'MutationEvent': (MutationEvent, encode_event, decode_event,),
    'UUID': (uuid.UUID, encode_uuid, decode_uuid,),
}


json = JsonCodec(extensions)
jsonlite = JsonCodec(extensions, include_type_annotations=False)
