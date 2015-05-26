import operator


def compare(*attributes):
    extract = operator.attrgetter(*attributes)
    return lambda a, b: extract(a) == extract(b)


class MutationBatch(object):
    def __init__(self, id, start_txid, end_txid, node, events=None):
        self.id = id
        self.start_txid = start_txid
        self.end_txid = end_txid
        self.node = node
        self.events = events if events is not None else []

    def __str__(self):
        return '%s (%s events from %s to %s on %s)' % (
            self.id,
            len(self.events),
            self.start_txid,
            self.end_txid,
            self.node,
        )

    def __eq__(self, other):
        if not isinstance(other, MutationBatch):
            return False
        else:
            return compare(
                'id',
                'start_txid',
                'end_txid',
                'node',
                'events',
            )(self, other)


class MutationEvent(object):
    def __init__(self, id, schema, table, operation, primary_key_columns, states, transaction_id, timestamp):
        self.id = id
        self.schema = schema
        self.table = table
        self.operation = operation
        self.primary_key_columns = primary_key_columns
        self.states = tuple(states)
        self.transaction_id = transaction_id
        self.timestamp = timestamp

    def __str__(self):
        return '%s: %s (%d) at %.2f to %s.%s' % (
            self.id,
            self.operation,
            self.transaction_id,
            self.timestamp,
            self.schema,
            self.table,
        )

    def __eq__(self, other):
        if not isinstance(other, MutationEvent):
            return False
        else:
            return compare(
                'id',
                'schema',
                'table',
                'operation',
                'primary_key_columns',
                'states',
                'transaction_id',
                'timestamp',
            )(self, other)
