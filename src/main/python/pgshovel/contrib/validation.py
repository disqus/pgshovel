from __future__ import absolute_import

import cPickle as pickle
import collections
import logging
import sys

import psycopg2

from pgshovel.consumer.snapshot import (
    SnapshotBuilder,
    build_snapshotter,
    build_tree,
)
from pgshovel.interfaces.groups_pb2 import GroupConfiguration
from pgshovel.snapshot import State
from pgshovel.utilities import import_extras
from pgshovel.utilities.commands import (
    Option,
    command,
)
from pgshovel.utilities.protobuf import BinaryCodec

with import_extras('lmdb'):
    import lmdb


logger = logging.getLogger(__name__)


def get_key(group, snapshot):
    return ('%s:%s' % (group, snapshot.key)).encode('utf-8')


class SnapshotHandler(object):
    """
    A simple snapshot handler that stores data using Symax LMDB (Lightning
    Memory-Mapped Database). This is primarily useful for testing.
    """
    def __init__(self, application, environment):
        self.application = application
        self.environment = environment
        self.builder = SnapshotBuilder()

    def __call__(self, group, configuration, cursor, events):
        with self.environment.begin(write=True) as transaction:
            for snapshot in self.builder(self.application, group, configuration, cursor, events):
                key = get_key(group, snapshot)

                previous = transaction.get(key)
                if previous is not None:
                    previous = pickle.loads(previous)
                    if previous.transaction.timestamp > snapshot.transaction.timestamp:
                        logger.debug('Skipping out of order snapshot for %r, taken at %r', key, snapshot.transaction)
                        continue
                    else:
                        logger.debug('Overwriting previous snapshot for %r, taken at %r', key, previous.transaction)

                transaction.put(key, pickle.dumps(snapshot), dupdata=False, overwrite=True)

    @classmethod
    def build(cls, application, path):
        # TODO: Expose options to `lmdb.open` as command line options.
        return cls(
            application,
            lmdb.open(
                path,
                map_size=int(1e9),  # totally arbitrary choice
            )
        )



def chunks(cursor, size=100):
    while True:
        chunk = cursor.fetchmany(size)
        if not chunk:
            return

        yield chunk


@command(
    description=\
        "Validate that all records from the origin database have been written "
        "to the LMDB replica with the correct data.",
    options=(
        Option('-v', '--verbose', action='store_true', help='print current and stored values of incorrect results'),
    ),
)
def validate(options, application, path, *groups):
    environment = lmdb.open(
        path,
        map_size=int(1e9),  # totally arbitrary choice
    )

    results = collections.defaultdict(set)
    codec = BinaryCodec(GroupConfiguration)

    with application, environment.begin(write=False) as transaction:
        # TODO: This could be made more efficient by performing group
        # validation in a subprocess (and maybe even performing each validation
        # chunk in a subprocess.)
        for group in groups:
            raw, _ = application.environment.zookeeper.get(application.get_group_path(group))
            configuration = codec.decode(raw)
            get_snapshot = build_snapshotter(build_tree(configuration.table))

            with psycopg2.connect(configuration.database.connection.dsn) as connection, connection.cursor() as cursor:
                # TODO: This would not be a smart query to execute on a
                # production data set, but is usable for testing. It would
                # probably make sense to get the maximum ID value here and then
                # split this into several range queries to avoid having one
                # very large transaction.
                statement = 'SELECT %s FROM %s' % (configuration.table.primary_key, configuration.table.name)
                cursor.execute(statement)

                for rows in chunks(cursor):
                    keys = [r[0] for r in rows]

                    with connection.cursor() as scursor:
                        snapshots = get_snapshot(application, scursor, keys)
                        for current in snapshots:
                            key = get_key(group, current)
                            stored = transaction.get(key)
                            if stored is not None:
                                stored = pickle.loads(stored)

                            correct = current.state == getattr(stored, 'state', None)
                            results[correct].add(key)
                            print >> sys.stderr, key, 'correct' if correct else 'incorrect'
                            if not correct and options.verbose:
                                print >> sys.stderr, 'current:', current
                                print >> sys.stderr, 'stored:', stored

        print len(results[True]), 'correct'
        print len(results[False]), 'incorrect'


if __name__ == '__main__':
    validate()
