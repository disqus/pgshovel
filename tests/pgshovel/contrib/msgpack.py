import uuid

from pgshovel.contrib.msgpack import codec
from pgshovel.events import (
    MutationBatch,
    MutationEvent,
)


def test_codec():
    event = MutationEvent(1, "public", "auth_user", "INSERT", (None, {"id": 1}), 1, 1)
    batch = MutationBatch(1, 1, 2, uuid.uuid1(), [event])
    payload = codec.encode(batch)
    decoded = codec.decode(payload)
    assert decoded == batch
