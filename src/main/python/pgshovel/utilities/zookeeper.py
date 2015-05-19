from __future__ import absolute_import

import logging
import pprint


logger = logging.getLogger(__name__)


class TransactionFailed(Exception):
    def __init__(self, results):
        super(TransactionFailed, self).__init__(pprint.pformat(results))
        self.results = results


def commit(transaction):
    logger.info(
        'Attempting to commit transaction %r (%s operations) using %r...',
        transaction,
        len(transaction.operations),
        transaction.client,
    )
    results = zip(transaction.operations, transaction.commit())
    if any(isinstance(result[1], Exception) for result in results):
        raise TransactionFailed(results)
    logger.info('Successfully committed transaction %r.', transaction)
    return results
