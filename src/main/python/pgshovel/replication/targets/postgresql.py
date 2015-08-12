import operator

from pgshovel.administration import get_managed_databases
from pgshovel.replication.targets.base import Target
from pgshovel.interfaces.replication_pb2 import State
from pgshovel.interfaces.streams_pb2 import MutationOperation
from pgshovel.utilities.conversions import column_converter
from pgshovel.utilities.postgresql import quote


def get_connection(cluster, dsn):
    _, connection = get_managed_databases(cluster, (dsn,)).items()[0]
    return connection


def get_identity_constraints(operation):
    assert operation.operation in (MutationOperation.UPDATE, MutationOperation.DELETE)

    constraints = []
    for key in operation.identity_columns:
        for column in operation.old.columns:
            if column.name == key:
                constraints.append((key, column_converter.to_python(column)[1]))
                break

    assert len(constraints) == len(operation.identity_columns)

    return constraints


def handle_insert(state, operation):
    statement = 'INSERT INTO {schema}.{table} ({columns}) VALUES ({placeholders})'.format(
        schema=quote(operation.schema),
        table=quote(operation.table),
        columns=', '.join(map(quote, map(operator.attrgetter('name'), operation.new.columns))),
        placeholders=', '.join(['%s' for _ in operation.new.columns]),
    )
    parameters = map(operator.itemgetter(1), map(column_converter.to_python, operation.new.columns))
    return statement, parameters


def handle_update(state, operation):
    constraints = get_identity_constraints(operation)

    statement = 'UPDATE {schema}.{table} SET ({columns}) = ({placeholders}) WHERE {constraints}'.format(
        schema=quote(operation.schema),
        table=quote(operation.table),
        columns=', '.join(map(quote, map(operator.attrgetter('name'), operation.new.columns))),
        placeholders=', '.join(['%s' for _ in operation.new.columns]),
        constraints=', '.join(['%s = %%s' % quote(c[0]) for c in constraints]),
    )
    parameters = map(operator.itemgetter(1), map(column_converter.to_python, operation.new.columns)) + [c[1] for c in constraints]
    return statement, parameters


def handle_delete(state, operation):
    constraints = get_identity_constraints(operation)

    statement = 'DELETE FROM {schema}.{table} WHERE {constraints}'.format(
        schema=quote(operation.schema),
        table=quote(operation.table),
        constraints=', '.join(['%s = %%s' % quote(c[0]) for c in constraints]),
    )
    parameters = [c[1] for c in constraints]
    return statement, parameters


mutation_handlers = {
    MutationOperation.INSERT: handle_insert,
    MutationOperation.UPDATE: handle_update,
    MutationOperation.DELETE: handle_delete,
}

class PostgreSQLTarget(Target):
    def __init__(self, cluster, set, connection):
        super(PostgreSQLTarget, self).__init__(cluster, set)

        # TODO: Switch this to used ManagedConnection? This should error if the
        # destination database changes between operations for some reason (even
        # after a crash.)
        self.connection = connection
        with self.connection.cursor() as cursor:
            cursor.execute('SET CONSTRAINTS ALL DEFERRED')
            cursor.execute('SET SESSION session_replication_role TO \'replica\'')

    def load(self, table, records):
        # TODO: This should lock and truncate table (check to make sure there
        # are no rows first?), as well as deal with dropping indexes, foreign
        # keys, etc for a more efficient load.
        with self.connection.cursor() as cursor:
            for record in records:
                # TODO: This would be better to move out of the loop, if
                # possible. The column list would need to be exported by the
                # loader itself. (This is probably necessary for loading from
                # files, anyway.)
                statement = 'INSERT INTO {schema}.{table} ({columns}) VALUES ({placeholders})'.format(
                    schema=quote(table.schema),
                    table=quote(table.name),
                    columns=', '.join(map(quote, map(operator.attrgetter('name'), record.columns))),
                    placeholders=', '.join(['%s' for _ in record.columns]),
                )
                cursor.execute(statement, map(operator.itemgetter(1), map(column_converter.to_python, record.columns)))

    def get_state(self):
        with self.connection.cursor() as cursor:
            cursor.execute(
                'SELECT state FROM {schema}.replication_state WHERE set = %s'.format(
                    schema=self.cluster.schema,
                ),
                (self.set,),
            )
            results = cursor.fetchall()
            if len(results) == 0:
                return None
            elif len(results) == 1:
                return State.FromString(results[0][0])
            else:
                raise AssertionError('too many rows returned')

    def set_state(self, state):
        if self.get_state():
            self._update_state(state)
        else:
            self._insert_state(state)

    def _insert_state(self, state):
        with self.connection.cursor() as cursor:
            cursor.execute(
                'INSERT INTO {schema}.replication_state (set, state) VALUES (%s, %s)'.format(
                    schema=self.cluster.schema,
                ),
                (self.set, bytearray(state.SerializeToString())),
            )

    def _update_state(self, state):
        with self.connection.cursor() as cursor:
            cursor.execute(
                'UPDATE {schema}.replication_state SET state = %s WHERE set = %s'.format(
                    schema=self.cluster.schema,
                ),
                (bytearray(state.SerializeToString()), self.set),
            )

    def apply(self, state, mutation):
        handler = mutation_handlers[mutation.operation]
        with self.connection.cursor() as cursor:
            cursor.execute(*handler(state, mutation))

    def commit(self, state):
        self.set_state(state)
        self.connection.commit()

    def rollback(self, state):
        self.connection.rollback()
        self.set_state(state)
        self.connection.commit()

    @classmethod
    def configure(cls, configuration, cluster, set):
        return cls(cluster, set, get_connection(cluster, configuration['dsn']))
