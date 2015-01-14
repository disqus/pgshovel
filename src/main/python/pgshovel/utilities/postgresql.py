import logging
import sys
import uuid

from pgshovel.utilities.exceptions import chained


logger = logging.getLogger(__name__)


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
