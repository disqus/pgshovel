from pgshovel.codecs import (
    json,
    jsonlite,
)
from tests.pgshovel.fixtures import batch_builder


def test_codec():
    batch = batch_builder(3)
    payload = json.encode(batch)
    decoded = json.decode(payload)
    assert decoded == batch


def test_lite_codec():
    batch = batch_builder(3)
    payload = jsonlite.encode(batch)
