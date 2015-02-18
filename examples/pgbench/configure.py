"""
Helper script that reads table configurations over stdin, adds the database
configuration (as provided by argv) and writes the group configuration to
stdout.
"""
import sys

from pgshovel.interfaces.groups_pb2 import (
    DatabaseConfiguration,
    GroupConfiguration,
    TableConfiguration,
)
from pgshovel.utilities.protobuf import TextCodec


table = TextCodec(TableConfiguration).decode(sys.stdin.read())

arguments = sys.argv[1:]

dsn = arguments[0]

try:
    name = arguments[1]
except IndexError:
    name = 'pgbench'

group = GroupConfiguration(
    database=DatabaseConfiguration(
        name=name,
        connection=DatabaseConfiguration.ConnectionConfiguration(
            dsn=dsn,
        ),
    ),
    table=table,
)

sys.stdout.write(TextCodec(GroupConfiguration).encode(group))
