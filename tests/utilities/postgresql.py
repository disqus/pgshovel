import psycopg2
import pytest

from pgshovel.utilities.postgresql import ManagedConnection
from tests.integration import TemporaryDatabase


@pytest.yield_fixture
def manager():
    with TemporaryDatabase('db') as database:
        manager = ManagedConnection(database.connection.dsn)
        with manager() as connection, connection.cursor() as cursor:
            cursor.execute("CREATE TABLE example (key varchar PRIMARY KEY, value varchar)")
            connection.commit()

        yield manager


def test_connection_reset_on_error(manager):
    with pytest.raises(psycopg2.Error):
        with manager() as connection, connection.cursor() as cursor:
            raise psycopg2.Error('There was an error')

    assert manager.closed, 'connection should be closed'

    with manager() as connection, connection.cursor() as cursor:
        cursor.execute('SELECT 1')
        assert cursor.fetchone() == (1,)
        connection.commit()

    assert not manager.closed, 'connection should still be open'


def test_connection_rollback_on_error(manager):
    with pytest.raises(AssertionError):
        with manager() as connection, connection.cursor() as cursor:
            cursor.execute('INSERT INTO example (key, value) VALUES (%s, %s)', ('foo', 'bar'))
            assert manager.status == psycopg2.extensions.TRANSACTION_STATUS_INTRANS
            raise AssertionError('There was an application error')

    assert not manager.closed, 'connection should still be open'
    assert manager.status == psycopg2.extensions.TRANSACTION_STATUS_IDLE

    with manager() as connection, connection.cursor() as cursor:
        cursor.execute('SELECT count(*) FROM example')
        assert cursor.fetchone() == (0,), 'no rows should exist in the table'
        connection.commit()


def test_connection_release_in_transaction(manager):
    with pytest.raises(RuntimeError):
        with manager() as connection, connection.cursor() as cursor:
            cursor.execute('INSERT INTO example (key, value) VALUES (%s, %s)', ('foo', 'bar'))
            assert manager.status == psycopg2.extensions.TRANSACTION_STATUS_INTRANS

    assert not manager.closed, 'connection should still be open'
    assert manager.status == psycopg2.extensions.TRANSACTION_STATUS_IDLE