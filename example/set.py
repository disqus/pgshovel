import sys

from pgshovel.interfaces.configurations_pb2 import (
    DatabaseConfiguration,
    TableConfiguration,
    ReplicationSetConfiguration,
)
from pgshovel.utilities.protobuf import TextCodec


dsn = sys.argv[1]


configuration = ReplicationSetConfiguration(
    database=DatabaseConfiguration(dsn=dsn),
    tables=[
        TableConfiguration(
            name='pgbench_accounts',
            primary_keys=['aid'],
            schema='public',
        ),
        TableConfiguration(
            name='pgbench_branches',
            primary_keys=['bid'],
            schema='public',
        ),
        TableConfiguration(
            name='pgbench_tellers',
            primary_keys=['tid'],
            schema='public',
        ),
    ],
)


sys.stdout.write(TextCodec(ReplicationSetConfiguration).encode(configuration))
