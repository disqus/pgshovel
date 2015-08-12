from pgshovel.interfaces.common_pb2 import (
    Column,
    Row,
    Snapshot,
    Timestamp,
)
from pgshovel.utilities.conversions import (
    RowConverter,
    to_snapshot,
    to_timestamp,
)
from tests.pgshovel.streams.fixtures import reserialize


def test_row_conversion():
    converter = RowConverter(sorted=True)  # maintain sort order for equality checks

    row = reserialize(
        Row(
            columns=[
                Column(name='active', boolean=True),
                Column(name='biography'),
                Column(name='id', integer64=9223372036854775807),
                Column(name='reputation', float=1.0),
                Column(name='username', string='bob'),
            ],
        ),
    )

    decoded = converter.to_python(row)
    assert decoded == {
        'id': 9223372036854775807,
        'username': 'bob',
        'active': True,
        'reputation': 1.0,
        'biography': None,
    }

    assert converter.to_protobuf(decoded) == row


def test_snapshot_conversion():
    assert to_snapshot('1:10:') == Snapshot(
        min=1,
        max=10,
    )


def test_snapshot_conversion_in_progress():
    assert to_snapshot('1:10:2,3,4') == Snapshot(
        min=1,
        max=10,
        active=[2, 3, 4],
    )


def test_timetamp_conversion():
    assert to_timestamp(1438814328.940597) == Timestamp(
        seconds=1438814328,
        nanos=940597057,  # this is different due to floating point arithmetic
    )
