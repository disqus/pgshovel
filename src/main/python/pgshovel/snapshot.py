import functools
import itertools
import operator
from collections import (
    defaultdict,
    namedtuple,
)
from datetime import datetime

from pgshovel.utilities.postgresql import pg_date_format


class Column(namedtuple('Column', 'table name')):
    def __new__(cls, table, name):
        return super(Column, cls).__new__(cls, table, name)

    def __str__(self):
        return '"%s"."%s"' % (self.table.alias, self.name)


class Join(namedtuple('Join', 'left right')):
    def __new__(cls, left, right):
        return super(Join, cls).__new__(cls, left, right)

    def forward(self):
        return 'LEFT OUTER JOIN {right.table} AS "{right.table.alias}" ON {left} = {right}'.format(
            left=self.left,
            right=self.right,
        )

    def backward(self):
        return 'INNER JOIN {left.table} AS "{left.table.alias}" ON {right} = {left}'.format(
            left=self.left,
            right=self.right,
        )


class Table(namedtuple('Table', 'schema name alias primary_key columns joins')):
    def __new__(cls, schema, name, alias, primary_key, columns, joins=None):
        return super(Table, cls).__new__(
            cls,
            schema,
            name,
            alias,
            primary_key,
            columns,
            joins if joins is not None else [],
        )

    def __str__(self):
        return '"%s"."%s"' % (self.schema, self.name,)


def build_tree(configuration):
    counters = defaultdict(itertools.count)
    alias = lambda c: "%s_%i" % (c.name, next(counters[c.name]))

    def traverse(configuration, parent=None):
        table = Table(
            configuration.schema,
            configuration.name,
            alias(configuration),
            configuration.primary_key,
            configuration.columns,
        )

        if len(configuration.joins) > 1:
            raise ValueError('Only one join per table is currently supported.')

        for join in configuration.joins:
            jtable = traverse(join.table, parent=table)

            table.joins.append(
                Join(
                    Column(table, configuration.primary_key),
                    Column(jtable, join.foreign_key),
                )
            )

        return table

    return traverse(configuration)


def build_statement(tree):
    """
    Builds a SQL statement that can be used to retrieve all objects in the
    group by primary key from a tree object.
    """
    columns = []
    joins = []
    order = []

    def traverse(table):
        key = '"%s"."%s"' % (table.alias, table.primary_key,)
        columns.append(key)
        for column in table.columns:
            columns.append('"%s"."%s"' % (table.alias, column,))

        order.append('%s ASC' % (key,))

        for join in table.joins:
            joins.append(join)
            traverse(join.right.table)

    traverse(tree)

    statement = 'SELECT {columns} FROM {root} AS "{root.alias}"'.format(
        root=tree,
        columns=', '.join(columns),
    )

    if joins:
        statement = "%s %s" % (statement, " ".join(map(operator.methodcaller("forward"), joins)))

    return statement + ' WHERE "{root.alias}"."{root.primary_key}" IN %s ORDER BY {order}'.format(
        root=tree,
        order=', '.join(order),
    )


REVERSE_STATEMENT_TEMPLATE = \
    'SELECT DISTINCT "{root.alias}"."{root.primary_key}" AS "key" ' \
    'FROM {table} AS "{table.alias}" ' \
     '{joins} '\
    'WHERE "{table.alias}"."{table.primary_key}" IN %s'


def build_reverse_statements(root):
    """
    Builds a mapping of table to sequence of SQL statements that can be used to
    retrieve the primary key for a group from the primary key of a child table.
    """
    statements = defaultdict(list)

    def traverse(table, path=()):
        statements[table.name].append(
            REVERSE_STATEMENT_TEMPLATE.format(
                root=root,
                joins=" ".join(map(operator.methodcaller("backward"), reversed(path))),
                table=table,
            ),
        )

        for join in table.joins:
            traverse(join.right.table, path + (join,))

    for join in root.joins:  # don"t start from the root
        traverse(join.right.table, path=(join,))

    return dict(statements)


class State(namedtuple("State", "data related")):
    def __new__(cls, data, related=None):
        return super(State, cls).__new__(
            cls,
            data,
            related if related is not None else {}
        )


def normalize(data):
    """
    Performs type conversions necessary to make data equivalent with trigger output.
    """
    for key, value in data.items():
        if isinstance(value, datetime):
            data[key] = pg_date_format(value)
    return data


def build_result_expander(root):
    """
    Returns a function that can be used to transform the result of a query made
    using the provided tree into an iterator of (key, State) pairs.
    """
    head = operator.itemgetter(0)

    def build(table, rows):
        columns = table.columns

        # Group the rows by the primary key of the active table, providing an
        # iterator that contains all of the rows related to this group.
        for key, group in itertools.groupby(rows, head):
            group = list(group)

            # To get the data for the node, take the first row in the group,
            # slice off the primary key (it is only present for grouping -- if
            # it should be a column value, it should be part of the
            # configuration's columns attribute) and associate the requested
            # column values to the result row's values at the same indices.
            state = State(normalize(dict(zip(columns, group[0][1:]))))

            if len(table.joins) > 1:
                raise ValueError('Only one join per table is currently supported.')

            # Then, we can build and attach all of the state for the child tables.
            for join in table.joins:
                # The view of the rows passed to the child should be limited to
                # the data that is pertinent to them (in this case, removing
                # the primary key and column data that are associated with the
                # parent table.)
                offset = 1 + len(columns)
                limited = map(lambda row: row[offset:], group)

                children = state.related[".".join((join.right.table.name, join.right.name))] = []
                for child_key, child_state in build(join.right.table, limited):
                    # If the key is None, this is a null result (introduced by
                    # using a LEFT OUTER JOIN) and should be skipped.
                    if child_key is not None:
                        children.append(child_state)

            yield key, state

    return functools.partial(build, root)


Transaction = namedtuple("Transaction", "id timestamp")
Snapshot = namedtuple("Snapshot", "key state transaction")


def build_snapshotter(tree):
    statement = build_statement(tree)
    expand = build_result_expander(tree)

    def snapshot(cursor, keys):
        if not keys:
            return

        # Fetch the transaction metadata.
        cursor.execute("SELECT txid_current(), extract(epoch from NOW()) * 1e6::bigint")
        (result,) = cursor.fetchall()
        transaction = Transaction(*result)

        seen = set()

        # Fetch all of the rows that exist.
        # TODO: It would be nice to make this a prepared statement instead.
        cursor.execute(statement, (tuple(keys),))
        for key, state in expand(cursor):
            yield Snapshot(key, state, transaction)
            seen.add(key)

        # Also return records for any missing rows.
        missing = set(keys) - seen
        for key in missing:
            yield Snapshot(key, None, transaction)

    return snapshot


if __name__ == "__main__":
    import psycopg2
    import sys
    from pgshovel.interfaces.groups_pb2 import TableConfiguration
    from pgshovel.utilities.protobuf import TextCodec

    connection = psycopg2.connect(sys.argv[1])
    table = TextCodec(TableConfiguration).decode(open(sys.argv[2]).read())
    snapshot = build_snapshotter(build_tree(table))

    with connection.cursor() as cursor:
        for result in snapshot(cursor, map(int, sys.argv[3:])):
            print result
