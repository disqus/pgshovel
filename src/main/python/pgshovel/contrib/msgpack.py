from __future__ import absolute_import

from uuid import UUID

from pgshovel.events import (
    MutationBatch,
    MutationEvent,
)
from pgshovel.utilities import import_extras

with import_extras('msgpack'):
    import msgpack


class MessagePackCodec(object):
    def __init__(self, extensions={}):
        self.__extensions_by_code = {}
        self.__extensions_by_type = {}
        for code, (type, encoder, decoder) in extensions.items():
            assert type not in self.__extensions_by_type, 'duplicate type registration: %s' % (type,)
            self.__extensions_by_code[code] = decoder
            self.__extensions_by_type[type] = code, encoder

    def __default(self, value):
        registration = self.__extensions_by_type.get(type(value))
        if registration is not None:
            code, encoder = registration
            return msgpack.ExtType(code, encoder(self, value))
        else:
            return value

    def encode(self, message):
        return msgpack.packb(message, default=self.__default, use_bin_type=True)

    def __ext_hook(self, code, data):
        decoder = self.__extensions_by_code.get(code)
        if decoder is None:
            raise TypeError('unregistered extension type: %s' % (code,))
        return decoder(self, data)

    def decode(self, payload):
        return msgpack.unpackb(payload, ext_hook=self.__ext_hook)


def encode_batch(codec, batch):
    return codec.encode((
        batch.id,
        batch.start_txid,
        batch.end_txid,
        batch.node,
        batch.events,
    ))


def decode_batch(codec, payload):
    return MutationBatch(*codec.decode(payload))


def encode_event(codec, event):
    return codec.encode((
        event.id,
        event.schema,
        event.table,
        event.operation,
        event.primary_key_columns,
        event.states,
        event.transaction_id,
        event.timestamp,
    ))


def decode_event(codec, payload):
    return MutationEvent(*codec.decode(payload))


codec = MessagePackCodec({
    0: (MutationBatch, encode_batch, decode_batch),
    1: (MutationEvent, encode_event, decode_event),
    2: (UUID, (lambda codec, uuid: uuid.bytes), lambda codec, payload: UUID(bytes=payload)),
})
