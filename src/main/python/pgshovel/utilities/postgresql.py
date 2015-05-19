import logging
import threading
import uuid
from contextlib import contextmanager

import psycopg2

from pgshovel.utilities.exceptions import chained


logger = logging.getLogger(__name__)


def quote(value):
    return '"%s"' % (value.replace('"', '""'),)


class UnrecoverableTransactionFailure(Exception):
    pass


class Transaction(object):
    def __init__(self, connection, name, unrecoverable_errors=(UnrecoverableTransactionFailure,)):
        self.connection = connection
        self.xid = self.connection.xid(0, 'pgshovel:%s' % (name,), uuid.uuid1().hex)

        logger.info('Beginning %s...', self)
        self.connection.tpc_begin(self.xid)
        self.unrecoverable_errors = unrecoverable_errors

    def __str__(self):
        return 'prepared transaction %s on %s' % (str(self.xid), self.connection.dsn)

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

    def __abort_on_failure(self, operation, callable):
        logger.info("Attempting to %s %s...", operation, self)
        try:
            callable()
        except Exception as error:
            logger.fatal(
                "Could not %s transaction %s due to error: %s."
                "The transaction will need to be manually recovered.\n",
                operation,
                self,
                error,
            )
            raise chained('FATAL ERROR: Encountered transaction failure that requires manual intervention!', RuntimeError)

    def prepare(self):
        logger.info('Preparing %s...', self)
        self.connection.tpc_prepare()

    def commit(self):
        self.__abort_on_failure('commit', self.connection.tpc_commit)
        logger.info('Succesfully committed %s.', self)

    def rollback(self):
        self.__abort_on_failure('rollback', self.connection.tpc_rollback)
        logger.info('Successfully rolled back %s.', self)


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

        # Barrier for when the `__connection` attribute is being modified.
        self.__connection_change_lock = threading.Lock()

        # Barrier to prevent multiple threads from utilizing the connection at
        # the same time to avoid interleaving transactions on the connection.
        self.__connection_usage_lock = threading.Lock()

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
        with self.__connection_change_lock:
            if self.__connection is None:
                return True
            return self.__connection.closed

    @property
    def status(self):
        with self.__connection_change_lock:
            if self.__connection is None:
                return None
            return self.__connection.get_transaction_status()

    @contextmanager
    def __call__(self, close=False):
        # TODO: Support the ability to be non-blocking.
        def close_connection():
            try:
                self.__connection.close()
            except Exception as error:
                logger.info('Could not close connection: %s', error, exc_info=True)
            finally:
                with self.__connection_change_lock:
                    self.__connection = None

        with self.__connection_usage_lock:
            with self.__connection_change_lock:
                if not self.__connection or self.__connection.closed:
                    logger.debug('Connecting to %s...', self.dsn)
                    self.__connection = psycopg2.connect(self.dsn)

            try:
                yield self.__connection
            except Exception as error:
                if isinstance(error, psycopg2.Error):
                    close_connection()
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

                if close:
                    close_connection()
