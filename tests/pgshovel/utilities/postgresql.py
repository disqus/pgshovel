import uuid

import psycopg2
import pytest

from pgshovel.utilities.postgresql import ManagedConnection



@pytest.yield_fixture
def managed_connection():
    dsn = 'postgresql://postgres@postgres'

    connection = psycopg2.connect(dsn + '/postgres')  # TODO join
    connection.autocommit = True
    with connection.cursor() as cursor:
        name = 'test_%s' % (uuid.uuid1().hex,)
        cursor.execute("CREATE DATABASE {name}".format(name=name))
    connection.close()

    managed_connection = ManagedConnection(dsn + '/' + name)
    with managed_connection() as connection, connection.cursor() as cursor:
        cursor.execute("CREATE TABLE example (key varchar PRIMARY KEY, value varchar)")
        connection.commit()

    yield managed_connection

    assert managed_connection.closed


def test_connection_reset_on_error(managed_connection):
    """
    Ensures that a managed connection is closed when a database error is
    encountered.
    """
    with pytest.raises(psycopg2.Error):
        with managed_connection() as connection, connection.cursor() as cursor:
            raise psycopg2.Error('There was an error')

    assert managed_connection.closed, 'connection should be closed'

    # Check to make sure that the connection can be recovered cleanly.
    with managed_connection() as connection, connection.cursor() as cursor:
        cursor.execute('SELECT 1')
        assert cursor.fetchone() == (1,)
        connection.commit()

    assert not managed_connection.closed, 'connection should still be open'

    with managed_connection(close=True):
        # TODO: eventually postgresql should send SIGQUIT during exit to kill clients
        pass  # close connection for the benefit of test teardown


def test_connection_rollback_on_error(managed_connection):
    """
    Ensures that an application error (not a database error) causes the active
    transaction to be rolled back, but not cause the connection to be reset.
    """
    with pytest.raises(AssertionError):
        with managed_connection() as connection, connection.cursor() as cursor:
            cursor.execute('INSERT INTO example (key, value) VALUES (%s, %s)', ('foo', 'bar'))
            assert managed_connection.status == psycopg2.extensions.TRANSACTION_STATUS_INTRANS
            raise AssertionError('There was an application error')

    assert not managed_connection.closed, 'connection should still be open'
    assert managed_connection.status == psycopg2.extensions.TRANSACTION_STATUS_IDLE

    with managed_connection(close=True) as connection, connection.cursor() as cursor:
        cursor.execute('SELECT count(*) FROM example')
        assert cursor.fetchone() == (0,), 'no rows should exist in the table'
        connection.commit()


def test_connection_release_in_transaction(managed_connection):
    """
    Ensures that leaving the managed connection without committing or rolling
    back the active transaction causes an error.
    """
    with pytest.raises(RuntimeError):
        with managed_connection() as connection, connection.cursor() as cursor:
            cursor.execute('INSERT INTO example (key, value) VALUES (%s, %s)', ('foo', 'bar'))
            assert managed_connection.status == psycopg2.extensions.TRANSACTION_STATUS_INTRANS

    assert not managed_connection.closed, 'connection should still be open'
    assert managed_connection.status == psycopg2.extensions.TRANSACTION_STATUS_IDLE

    with managed_connection(close=True):
        # TODO: eventually postgresql should send SIGQUIT during exit to kill clients
        pass  # close connection for the benefit of test teardown
