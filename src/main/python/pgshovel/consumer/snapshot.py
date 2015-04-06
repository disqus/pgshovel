import itertools
from cPickle import loads
import logging
import operator
import json
import threading
from collections import defaultdict

from pgshovel.consumer.handler import Handler
from pgshovel.snapshot import (
    Snapshot,
    State,
    Transaction,
    build_tree,
    build_snapshotter,
    build_reverse_statements,
)


logger = logging.getLogger(__name__)


class IncompatibleConfigurationError(Exception):
    """
    Error raised when a snapshot builder cannot be constructed for a configuration.
    """


def create_simple_snapshot_builder(configuration, compact=True):
    """
    Creates and returns a function that transforms an event (caused by a
    mutation trigger) into a snapshot object.
    """
    if configuration.table.joins:
        raise IncompatibleConfigurationError('Cannot create snapshot builder for configurations that include joins')

    columns = set(configuration.table.columns)

    def extract_key(state):
        return state[configuration.table.primary_key]

    def reduce_state(state):
        return dict([item for item in state.items() if item[0] in columns])

    def get_transaction(event):
        return Transaction(*event.data.transaction)

    def insert_snapshot(event):
        state = event.data.states.new
        return (
            Snapshot(
                key=extract_key(state),
                state=State(reduce_state(state)),
                transaction=get_transaction(event),
            ),
        )

    def update_snapshot(event):
        states = event.data.states
        transaction = get_transaction(event)
        snapshots = (
            Snapshot(
                key=extract_key(states.new),
                state=State(reduce_state(states.new)),
                transaction=transaction,
            ),
        )

        if extract_key(states.new) != extract_key(states.old):
            snapshots = snapshots + (
                Snapshot(
                    key=extract_key(states.old),
                    state=None,
                    transaction=transaction,
                ),
            )

        return snapshots

    def delete_snapshot(event):
        state = event.data.states.old
        return (
            Snapshot(
                key=extract_key(state),
                state=None,
                transaction=get_transaction(event),
            ),
        )

    handlers = {
        'INSERT': insert_snapshot,
        'UPDATE': update_snapshot,
        'DELETE': delete_snapshot,
    }

    def resolve(application, cursor, events):
        for event in events:
            handler = handlers[event.data.operation]
            for snapshot in handler(event):
                yield snapshot

    def resolve_compact(application, cursor, events):
        snapshots = {}
        for event in events:
            handler = handlers[event.data.operation]
            for snapshot in handler(event):
                # TODO: This assumes in-order delivery, it might be safer to
                # use the transaction time instead as a comparator?
                snapshots[snapshot.key] = snapshot

        for snapshot in snapshots.itervalues():
            yield snapshot

    # TODO: Enabling compaction should probably be a part of the group
    # configuration.
    return resolve_compact if compact else resolve


def create_complex_snapshot_builder(configuration):
    """
    """
    tree = build_tree(configuration.table)
    reverse = build_reverse_statements(tree)
    snapshot = build_snapshotter(tree)

    def build_key_extractor():
        """
        Builds a function that can be used to return all of the primary keys
        for a table associated with a transaction event.
        """
        primary_keys = defaultdict(set)

        def collect_primary_keys(table):
            primary_keys[table.name].add(table.primary_key)
            for join in table.joins:
                collect_primary_keys(join.right.table)

        collect_primary_keys(tree)

        # Create a map of functions that will extract the key item from the
        # transaction state.
        extractors = {}
        for table, keys in primary_keys.items():
            extractors[table] = map(operator.itemgetter, keys)

        def extract(event):
            functions = extractors[event.data.table]
            keys = set()
            for state in filter(None, event.data.states):
                keys.update(f(state) for f in functions)
            return keys

        return extract

    extract = build_key_extractor()

    def resolve(application, cursor, events):
        # Gather all of the events by the tables that they were performed on.
        sources = defaultdict(list)
        for event in events:
            sources[event.data.table].append(event)

        keys = set()

        # Special case the root of the tree (we don't have to do any sort of
        # intermediate step to find the primary key for the root table -- it's
        # just the primary key that is included in the event data.)
        for event in sources.pop(tree.name, []):
            keys.update(extract(event))

        # For the remainder of the tables, fetch the primary key for the root
        # of their trees, eventually resulting in a list of all keys that were
        # affected by the collection of events in this batch.
        for table, events in sources.iteritems():
            child_keys = set()
            for event in events:
                child_keys.update(extract(event))

            for statement in reverse[table]:
                tup = tuple(child_keys)
                logger.debug('Retrieving root key for %s records in %r...', len(tup), table)
                cursor.execute(statement, (tup,))

                res = [r[0] for r in cursor.fetchall()]
                logger.debug('Retrieved %s root keys from %s inputs.', len(res), len(tup))
                keys.update(res)

        return snapshot(application, cursor, keys)

    return resolve


SNAPSHOT_BUILDER_FACTORIES = (
    create_simple_snapshot_builder,
    create_complex_snapshot_builder,
)


def create_snapshot_builder(configuration, factories=SNAPSHOT_BUILDER_FACTORIES):
    """
    Creates a snapshot builder for the provided configuration.

    This method attempts to use the most lightweight snapshot method possible
    for the configuration.
    """
    for factory in factories:
        try:
            return factory(configuration)
        except IncompatibleConfigurationError:
            pass

    raise ValueError('Could not find compatible snapshot builder for configuration')


class SnapshotBuilder(object):
    """
    Converts a transaction event into a snapshot, using the most appropriate
    snapshot construction method possible.
    """
    def __init__(self):
        self.__builder_cache = {}

    def __call__(self, application, group, configuration, cursor, events):
        # WARNING: This assumes that the ``configuration`` is immutable!
        ident = id(configuration)
        builder = self.__builder_cache.get(ident)
        if builder is None:
            # TODO: This should do some sort of LRU eviction to avoid keeping
            # builders around forever.
            builder = self.__builder_cache[ident] = create_snapshot_builder(configuration)

        snapshots = list(builder(application, cursor, events))
        logger.debug('Converted %s events to %s snapshots.', len(events), len(snapshots))
        return snapshots


class StreamSnapshotHandler(Handler):
    """
    Writes event payloads to a stream.
    """
    def __init__(self, application, stream, template):
        self.application = application
        self.stream = stream
        self.template = template
        self.builder = SnapshotBuilder()

        self.__lock = threading.Lock()

    def __call__(self, group, configuration, cursor, events):
        with self.__lock:
            write = self.stream.write
            template = self.template
            snapshots = self.builder(self.application, group, configuration, cursor, events)
            for snapshot in snapshots:
                write(template.format(group=group, configuration=configuration, snapshot=snapshot))
            self.stream.flush()

    @classmethod
    def build(cls, application, path='/dev/stdout', template='{group} {snapshot}\n'):
        return cls(application, open(path, 'w'), template)
