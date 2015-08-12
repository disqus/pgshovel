import logging
import uuid
from contextlib import contextmanager

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


def txid_visible_in_snapshot(txid, snapshot):
    if txid < snapshot.min:
        return True
    elif txid >= snapshot.max:
        return False
    return txid not in set(snapshot.active)
