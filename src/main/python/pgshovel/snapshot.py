import collections
from contextlib import contextmanager

from psycopg2.extensions import ISOLATION_LEVEL_REPEATABLE_READ


@contextmanager
def prepare(connection, table):
    """
    Prepares a function that can be used to take a consistent snapshot of a set
    of capture groups by their primary keys.
    """
    connection.set_session(
        isolation_level=ISOLATION_LEVEL_REPEATABLE_READ,  # READ_COMMITTED might work here too, but being safe.
        readonly=True,
    )

    # TODO: This doesn't play well with self joins right now -- duplicate
    # tables will need to be aliased (this should be a straightforward change.)
    # TODO: This also doesn't handle 1:N joins right now.
    joins = []
    columns = []

    Transaction = collections.namedtuple('Transaction', 'id timestamp')
    Snapshot = collections.namedtuple('Snapshot', 'key state transaction')

    def collect(table):
        for column in sorted(set(tuple(table.columns) + (table.primary_key,))):
            columns.append((table.name, column))

        for join in table.joins:
            joins.append('LEFT OUTER JOIN {join.table.name} ON {table.name}.{table.primary_key} = {join.table.name}.{join.foreign_key}'.format(join=join, table=table))
            collect(join.table)

    collect(table)

    statement = 'SELECT {table.name}.{table.primary_key} as key, {columns} FROM {table.name}'.format(
        columns=', '.join('{0}.{1} as {0}__{1}'.format(*i) for i in columns),
        table=table,
    )

    if joins:
        statement = ' '.join((statement, ' '.join(joins)))

    statement = statement + ' WHERE {table.name}.{table.primary_key} IN %s'.format(table=table)

    with connection.cursor() as cursor:
        def snapshot(keys):
            # Fetch the transaction metadata.
            cursor.execute('SELECT txid_current(), extract(epoch from NOW())')
            (result,) = cursor.fetchall()
            transaction = Transaction(*result)

            seen = set()

            # Fetch all of the rows that exist.
            # TODO: Actually make this a real prepared statement.
            cursor.execute(statement, (tuple(keys),))
            for row in cursor:
                result = collections.defaultdict(dict)
                for (table, name), value in zip(columns, row[1:]):
                    result[table][name] = value
                key = row[0]
                seen.add(key)
                yield Snapshot(key, dict(result), transaction)

            # Also return records for any missing rows.
            missing = set(keys) - seen
            for key in missing:
                yield Snapshot(key, None, transaction)

            # Ensure that long lived connections are not stuck idle in transaction.
            connection.commit()

        yield snapshot


if __name__ == '__main__':
    import psycopg2
    import sys
    from pgshovel.interfaces.groups_pb2 import TableConfiguration
    from pgshovel.utilities.protobuf import TextCodec

    table = TextCodec(TableConfiguration).decode(open(sys.argv[2]).read())
    with prepare(psycopg2.connect(sys.argv[1]), table) as snapshot:
        for result in snapshot(map(int, sys.argv[3:])):
            print result
