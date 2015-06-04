from pgshovel.contrib.msgpack import codec
from tests.pgshovel.fixtures import batch_builder


def test_codec():
    batch = batch_builder(3)
    payload = codec.encode(batch)
    decoded = codec.decode(payload)
    assert decoded == batch
