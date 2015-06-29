Testing
#######

Build Images
------------

::

    make images

ZooKeeper
---------

::

    docker run -d --name zookeeper confluent/zookeeper

Kafka
-----

::

   docker run -d --name kafka --link zookeeper:zookeeper confluent/kafka

PostgreSQL
----------

::

    docker run -d --name postgres postgres-pgshovel
    docker exec postgres psql -c "CREATE DATABASE example" postgres://postgres@/postgres
    docker exec -i postgres psql postgres://postgres@/example < schema.sql

Replication Cluster
-------------------

::

    docker run --rm --link zookeeper:zookeeper --link postgres:postgres pgshovel pgshovel --zookeeper-hosts zookeeper:2181 cluster initialize
    docker run --rm --link zookeeper:zookeeper --link postgres:postgres -i pgshovel pgshovel --zookeeper-hosts zookeeper:2181 set create example < set.pgshovel

.. tip::

    To make code changes without necessitating a rebuild, mount the repository
    root directory as ``/usr/src/app``::

        ``-v $(git rev-parse --show-toplevel):/usr/src/app``

PgQ Ticker
----------

(This must follow ``pgshovel cluster initialize`` to ensure that the ``pgq`` extension is installed.)

::

    docker run -d --name pgq-ticker --link postgres:postgres python-pgqueue host=postgres dbname=example user=postgres password=

Relay
-----

::

    docker run --rm --link zookeeper:zookeeper --link postgres:postgres --link kafka:kafka pgshovel pgshovel-kafka-relay --zookeeper-hosts zookeeper:2181 --kafka-hosts kafka:9092 example

Add Data
--------

::

    docker exec postgres psql -c "INSERT INTO auth_user (username) VALUES (random()::text)" postgres://postgres@/example
