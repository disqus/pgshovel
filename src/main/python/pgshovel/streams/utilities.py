import numbers

from pgshovel.streams.interfaces_pb2 import (
    Column,
    Row,
    Snapshot,
    Timestamp,
)


def to_timestamp(value):
    return Timestamp(
        seconds=int(value),
        nanos=int((value % 1) * 1e9),
    )


def to_snapshot(value):
    xmin, xmax, xip = value.split(':')
    return Snapshot(
        min=int(xmin),
        max=int(xmax),
        active=map(int, filter(None, xip.split(','))),
    )


class ColumnConverter(object):
    def __init__(self):
        self.conversions = {
            basestring: lambda value: {'string': value.encode('utf8')},
            bool: lambda value: {'boolean': value},
            float: lambda value: {'float': value},
            numbers.Integral: lambda value: {'integer64': value},
        }

    def to_python(self, value):
        type = value.WhichOneof('value')
        if type is not None:
            result = getattr(value, type)
        else:
            result = None
        return (value.name, result)

    def to_protobuf(self, value):
        key, value = value

        parameters = {}
        if value is not None:
            for type, converter in self.conversions.items():
                if isinstance(value, type):
                    parameters.update(converter(value))
                    break

        return Column(name=key, **parameters)


column_converter = ColumnConverter()


class RowConverter(object):
    def __init__(self, sorted=False):
        self.sorted = sorted

    def to_protobuf(self, value):
        columns = map(column_converter.to_protobuf, value.items())
        if self.sorted:
            columns = sorted(columns, key=lambda column: column.name)
        return Row(columns=columns)

    def to_python(self, value):
        return dict(map(column_converter.to_python, value.columns))


row_converter = RowConverter()
