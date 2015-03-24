import logging
import sys
import threading
import uuid
from contextlib import contextmanager
from datetime import timedelta

import psycopg2

from pgshovel.utilities.exceptions import chained


logger = logging.getLogger(__name__)


def pg_date_format(value):
    """
    Converts a datetime to the ISO 8601-ish format used by PostgreSQL.

    The exact implementation of the format is undocumented, but it generally
    optimizes by dropping off implied characters (such as right padding on
    microsecond values, or the minute part of timezone specifications that
    contain only hours.)
    """
    bits = []

    bits.append(value.strftime('%Y-%m-%d %H:%M:%S'))

    if value.microsecond:
        bits.append(('.%06d' % value.microsecond).rstrip('0'))

    # The basic logic here was extracted from
    # https://hg.python.org/cpython/file/24d4152b0040/Lib/datetime.py#l1163 and
    # slightly modified for Python 2.X compatibility.
    offset = value.utcoffset()
    if offset:
        if offset.days < 0:
            sign = "-"
            offset = -offset
        else:
            sign = "+"

        hours, minutes = divmod(offset.total_seconds(), timedelta(hours=1).total_seconds())
        assert not minutes % 60, "whole minute"
        minutes //= 60
        assert 0 <= hours < 24
        bits.append('%s%02d' % (sign, hours))
        if minutes:
            bits.append(':%02d' % (minutes,))

    return ''.join(bits)


class UnrecoverableTransactionFailure(Exception):
    pass


class Transaction(object):
    def __init__(self, connection, name, stream=sys.stderr, unrecoverable_errors=(UnrecoverableTransactionFailure,)):
        self.connection = connection
        self.xid = self.connection.xid(0, 'pgshovel:%s' % (name,), uuid.uuid1().hex)
        self.connection.tpc_begin(self.xid)
        self.stream = stream
        self.unrecoverable_errors = unrecoverable_errors

    def __enter__(self):
        self.prepare()

    def __exit__(self, type, value, traceback):
        # If this is an exception that has failed due to the failure of another
        # two-phase transaction (or another exception that has been marked as
        # unrecoverable), don't perform either a commit or rollback of the 2PC
        # state, and allow the user to choose the correct recovery method.
        if type and issubclass(type, self.unrecoverable_errors):
            return

        if any((type, value, traceback)):
            self.rollback()
        else:
            self.commit()

    def __abort_on_failure(self, callable):
        self.stream.write("Attempting to perform %r for %s...\n" % (callable, self.xid))
        try:
            callable()
        except Exception as error:
            self.stream.write(
                "Could not perform %r for %s on %r due to error: %s."
                "The transaction will need to be manually recovered.\n" % (
                callable,
                self.xid,
                self.connection,
                error,
            ))
            raise chained('FATAL ERROR: Encountered transaction failure that requires manual intervention!', RuntimeError)

    def prepare(self):
        self.stream.write('Preparing transaction %s on %r...\n' % (self.xid, self.connection))
        self.connection.tpc_prepare()

    def commit(self):
        self.__abort_on_failure(self.connection.tpc_commit)

    def rollback(self):
        self.__abort_on_failure(self.connection.tpc_rollback)


@contextmanager
def managed(transactions):
    """
    Prepares a sequence of transactions (in the order they were provided),
    committing them all if the managed block executes successfully, otherwise
    rolling them all back.

    If any of the ``commit`` or ``rollback`` operations fail, the transactions
    will need to be manually recovered by an administrator.
    """
    prepared = []

    try:
        # Prepare all of the database transactions...
        for transaction in transactions:
            transaction.prepare()
            prepared.append(transaction)
        yield
    except Exception:
        # If an exception is raised (for any reason) during the transaction
        # block, roll back all of the transations that have already been
        # prepared.
        for transaction in prepared:
            transaction.rollback()

        raise

    # If we got this far, then everything is OK and we can finalize all
    # transactions.
    for transaction in prepared:
        transaction.commit()


class ManagedConnection(object):
    STATUS_IN_TRANSACTION = frozenset((
        psycopg2.extensions.TRANSACTION_STATUS_INERROR,
        psycopg2.extensions.TRANSACTION_STATUS_INTRANS,
    ))

    def __init__(self, dsn):
        self.dsn = dsn

        self.__connection = None

        # Barrier for when the `__connection` attribute is being modified
        # set/unset is being mutated to avoid data races.
        self.__meta_lock = threading.Lock()

        # Barrier to prevent multiple threads from using the connection at
        # once.
        self.__lock = threading.Lock()

    def __str__(self):
        return '%s' % (self.dsn,)

    def __repr__(self):
        return '<%s: %s (%s)>' % (
            type(self).__name__,
            self.dsn,
            self.__connection or 'closed',
        )

    @property
    def closed(self):
        with self.__meta_lock:
            if self.__connection is None:
                return True
            return self.__connection.closed

    @property
    def status(self):
        with self.__meta_lock:
            if self.__connection is None:
                return None
            return self.__connection.get_transaction_status()

    @contextmanager
    def __call__(self):
        # TODO: Support the ability to be non-blocking.
        with self.__lock:
            with self.__meta_lock:
                if not self.__connection or self.__connection.closed:
                    self.__connection = psycopg2.connect(self.dsn)

            try:
                yield self.__connection
            except Exception as error:
                if isinstance(error, psycopg2.Error):
                    try:
                        self.__connection.close()
                    except Exception as error:
                        logger.info('Could not close connection during recovery: %s', error, exc_info=True)
                    finally:
                        with self.__meta_lock:
                            self.__connection = None
                else:
                    if self.__connection.get_transaction_status() in self.STATUS_IN_TRANSACTION:
                        logger.warning('Forcing connection rollback after exception thrown in connection block: %s', error, exc_info=True)
                        self.__connection.rollback()
                raise
            else:
                # Check to make sure the client didn't leave the connection in a dirty state.
                if self.__connection.get_transaction_status() in self.STATUS_IN_TRANSACTION:
                    # We need to roll back the transaction in case the
                    # exception we raise below is caught and the manager is
                    # used again.
                    self.__connection.rollback()
                    raise RuntimeError("Did not commit or rollback open transaction before returning connection. Were you born in a barn? (We rolled it back for you, anyway.)")
