NOTICE: Deprecated
##################
This project is deprecated and no longer actively maintained by Disqus.

pgshovel
########

.. image:: https://travis-ci.org/disqus/pgshovel.svg?branch=master
    :target: https://travis-ci.org/disqus/pgshovel

``pgshovel`` is a `change data capture`_ system for PostgreSQL_, built on top of
`Apache ZooKeeper`_, PgQ (part of SkyTools_), and database triggers.

It is inspired by LinkedIn's Databus_ project, and trigger-based asynchronous
replication solutions for PostgreSQL such as Slony_ and Londiste (also part of
SkyTools).

ZooKeeper is used for storing cluster configurations and ensuring that the
configurations remain synchronized with the state of all databases in the
cluster. PgQ is used for buffering mutation records until they are collected by
the relay and forwarded to their final destination.

Overview
========

``pgshovel`` is a system which replicates mutations (``INSERT``, ``UPDATE`` and ``DELETE`` operations) made to a PostgreSQL database out of the database and into an ordered stream of transaction-aware mutation messages. These messages can then be consumed by 3rd party systems as needed. Commonly, the mutation stream is consumed by data warehouse operations, cache updating services, search index services or can even be processed into other new streams.

At Disqus, ``pgshovel`` is actively being developed to replicate changes from our master PostgreSQL database, containing the core commenting and other relational data. The main consumer of this replicated data is our data warehousing system. Additionally, there is further planned work to use the replicated data to power a denormalized objects database, offering highly available low-latency access to object data.

Concepts
========

``pgshovel`` has a few core concepts which are important to understanding it:

Cluster
-------

A cluster contains a collection of replication sets.

Replication Set
---------------

A replication set is a collection of tables that are replicated together in a fashion that preserves data consistency.

Relay
-----

Relays are responsible for extracting queued raw mutations made against the tables defined in a replication set and then logging (relaying) them to a final destination. **There is exactly one relay for each replication set**.

Replication
-----------

The replication system is a collection of tools which handle replicating a stream of mutation into a new target, i.e. Cassandra or S3.

Within replication there are 4 main concepts:

Loader
~~~~~~

A loader is responsible for loading data from the source database from tables contained in a replication set, for the purpose of later being loaded into the replication target.

Stream
~~~~~~

Stream of ``BatchOperation`` messages produced by the relay for a replication set which will eventually be replicated into the target.

Target
~~~~~~

Destination of replicated data, i.e. Cassandra or Postgres. Initial versions of records are loaded into the target as provided by the loader, and then continued changes are applied via the stream.

Worker
~~~~~~

The worker is a command line tool which configures and runs the target.

Getting Started
===============

Installation Requirements
-------------------------

``pgshovel`` requires the following:

* A running ZooKeeper cluster.
* A running ``PgQ`` ticker (such as pgqd_) for all databases.
* PostgreSQL must have:

  * the SkyTools extension installed,
  * been compiled with ``--with-python``,
  * a non-zero value for the ``max_prepared_transactions`` configuration
    parameter. (This is required to ensure that all databases have been
    configured with the same version of the replication set configuration.)

You will also need to install ``pgshovel`` in your system. As of this writing it is not available on PyPI, so you will need to download it and run ``python setup.py install``.

All ``pgshovel`` administrative actions are performed via the ``pgshovel-admin`` tool, which comes with the ``pgshovel`` package once installed. Please consult ``pgshovel-admin`` for commands and other options.

Creating a Cluster
------------------

To begin, initialize a cluster.
::

    pgshovel-admin cluster initialize

Then create a replication set (here named ``example``) based on the configuration file.

::

    pgshovel-admin set create example /path/to/configuration.pgshovel


The configuration file is the protobuf text representation of a ``ReplicationSet``_ config object. Please see the `protobuf message definition`_ and `example set builder tool`_ for more information.

Now created, the set should be visible to the ``list`` command.

::

    pgshovel-admin set list


And the replication set may also be inspected via the ``inspect`` command.

::

    pgshovel-admin set inspect example

Running a Relay
---------------

Now, with the replication set all set up, you can run a relay process.

::

    pgshovel-relay configuration.yml example

The configuration file defines the relay object that should be run, as well its configuration.


Replication Worker
------------------

A replication worker is used to replicate a stream of batch operations into a new database replication target. It is started with the ``pgshovel-replicate`` command, passing the path to the replication config file as well as the replicaion set name.

::

    python-replicate example/configurations/replication.yml example

The config file looks very similar to the relay config file, though it contains three sections: ``loader``, ``stream``, and ``target``, corresponding to the three components of replication as described in the "Concepts" section above.

The replication feature is ongoing, so further documentation of the API is unavailable until the API becomes stable.

Results
-------

That's it!  The tables defined in the ``example`` replication set now have their mutations replicated out of PostgreSQL by the relay.  Additionally, the replication worker bootstrapped a new target and is now applying any new mutations to the target data store.

Further Administration
======================

``pgshovel`` also supports updates to its configuration via a variety of tasks.

Updating a Replication Set
--------------------------

If for any reason you need to update a replication set, you may do so using the ``update`` command, replacing the existing configuration with the one contained in the new config file.

::

    pgshovel-admin set update /path/to/configuration.pgshovel

Dropping a Replication Set
--------------------------

Replication sets may be dropped via the ``drop`` command.

::

    pgshovel-admin set drop example

Further Configuration and Development
======================================

In order to run a relay or replication worker, you need to specify a configuration file for their respective command line tool. This section documents the format of those configuration files, as well as information on writing your own components for both systems.

Batch Operations
----------------

The relay writes, and the replication stream reads, streams of batch operation messages. An ordered sequence of these operations describe a batch of mutations made against the tables contained in a replication set. A batch operation is a message communicating one of 4 possible actions:

1. ``BeginOperation``
2. ``MutationOperation``
3. ``CommitOperation``
4. ``RollbackOperation``

A batch of mutations start with a ``BeginOperation``. It is then followed up by zero-to-many ``MutationOperation`` messages, signifying a mutation that was made to a table. The ``MutationOperation`` messages are followed by either a ``CommitOperation`` signifying the mutation batch was successfully extracted from PostgreSQL.

If a ``RollbackOperation`` is found, it signifying there was an error processing the batch of mutations and the previous ``MutationOperation`` messages should not be applied.

Please note that all of the mutations contained in the batch, even if it has a ``RollbackOperation`` at the end, did actually occur and were committed to the table in PostgreSQL.  However, the only *consistent* view of a table's data is after the batch is completed with a ``CommitOperation`` and all mutations have been applied from that batch. If a portion of the total mutations in a batch are applied to a replication target, then the state of the data in the target may be invalid or inconsistent. This is due to the mutations applied being part of PostgreSQL transactions that occurred concurrently ont the source database, but the final results of which were not visible to other transactions until their transactions comitted. For more information, please see the `PostgreSQL docs for transaction iolation`_.

Relay Configuration
-------------------

The relay takes a yaml configuration file, which is in the following format.

::

    stream:
        path: module.path.to:WriterObject
        configuration:
            key: value
            key2: value2


The relay will attempt to load the Writer object defined at ``path``, calling ``.configure(configuration)`` on it. ``configuration`` is a ``dict`` containing the keys and values defined via the ``configuration`` key of the config file. This ``configure`` method is responsible for returning the newly constructed writer instance.

For instance, to use the built-in ``KafkaWriter`` at the hostname ``kafka``, use the following config file.

::

    stream:
        path: pgshovel.relay.streams.kafka:KafkaWriter
        configuration:
            hosts: kafka


Once started, the relay worker relays raw database mutations and writes them to the output stream. A Writer instance just needs to respond to the ``.push(messages)`` API, where ``messages`` is a sequence of batch operation objects.

Replication Configuration
-------------------------

The replication config file looks very similar to the relay config file, though it contains three sections: ``loader``, ``stream``, and ``target``, corresponding to the three components of replication as described in the "Concepts" section above.

::

    loader:
        path: module.path.to:Loader

    stream:
        path: module.path.to:Stream
        configuration:
            key: value

    target:
        path: module.path.to:Target
        configuration:
            key: value
            key2: value2

Like the relay config, the component defined at ``path`` has ``.configure(configuration)`` called on it, and the method must return a new instance of that component.

For example, here is a configuration file which loads data using the simple loader, streams in further mutations via the Kafka stream and replicates those changes to the PostgreSQLtarget.

::

    loader:
        path: pgshovel.replication.loaders.simple:SimpleLoader

    stream:
        path: pgshovel.replication.streams.kafka:KafkaStream
        configuration:
            hosts: kafka

    target:
        path: pgshovel.replication.targets.postgresql:PostgreSQLTarget
        configuration:
            dsn: postgres:///destination

Operations
==========

Upgrades
--------

.. todo:: Fix node watch issue in relay, update this to reflect automatic restart.

Monitoring
----------

PgQ
~~~

The mutation log (where mutation events are buffered before being forwarded by
the relay) can be monitored using the `Diamond PgQ Collector`_, or any other
tools designed for monitoring queue consumption and throughput.

PgQ provides many useful data points, including pending (unconsumed) events,
throughput rates, replication lag, and other metrics.

Relay
~~~~~

It is highly recommended to use Raven_ to report application warnings and
errors to a Sentry_ installation by providing a custom `logging configuration
file`_ in your pgshovel `Configuration`_ file.

The ``raven`` Python module for reporting to Sentry is installed by default
with the Docker image. The necessary dependencies for reporting can also be
installed as a ``setuptools`` extra with ``pip install pgshovel[sentry]``.

Planned Replica Promotion
-------------------------

.. todo:: Rewrite this as part of the tutorial using pgbench after the replication worker is done.

Unplanned Replica Promotion
---------------------------

.. todo:: Rewrite this as part of the tutorial using pgbench after the replication worker is done.

Comparison with Logical Decoding
================================

PostgreSQL, beginning with 9.4, provides a functionality called `logical
decoding`_ which can be used to access a change stream of data from a
PostgreSQL database. However, trigger-based replication has advantages over
logical decoding in a few select use cases:

* You only want to monitor specific tables, and not all of the columns within
  those tables. (For instance, you'd like to avoid creating mutation records
  for updates to denormalized data.)
* You run an older version of PostgreSQL (and don't intend to -- or cannot --
  upgrade in the near future.)

However, trigger-based replication suffers in environments that experience high
sustained write loads due to write amplification -- every row affected by a
mutation operation must be recorded to the event table, and incurs all of the
typical overhead of a database write.

In write-heavy environments, it is typically a better choice to use logical
decoding (assuming you can run PostgreSQL 9.4), foregoing some configuration
flexibility for increased throughput.

A similar project that utilizes logical decoding rather than trigger-based
replication is `Bottled Water`_.

Development
===========

The easiest way to run the project for development is via ``docker-compose``.

.. todo:: Include more details after the replication worker is complete.

The test suite also utilizes ``docker-compose`` for running integration tests.
However, it runs using a separate ephemeral cluster which is destroyed after
the completion of the test run to decrease the likelihood of transient state
affecting subsequent test runs. (This may require you to increase the amount of
memory allocated for boot2docker, if you are on OS X.)

To run the test suite::

    make test

The test suite can also be run against a currently running cluster, skipping
the ephemeral cluster teardown and setup::

    docker-compose run --rm --entrypoint=python pgshovel setup.py test

Dependency Versioning
---------------------

``pgshovel`` is intended to be used as both a client library as well as a
standalone application. As such, all dependencies should be declared in
``setup.py`` with both a loose version range (to increase compatibility when
used as a client library), as well as a specific version tag (to decrease the
likelihood of issues arising due to dependency version inconsistencies when
used as a standalone application.)

The ``requirements.txt`` can be rebuilt from the specifications in the
``setup.py`` script with the following command::

    make requirements.txt

License
-------

``pgshovel`` is licensed under the Apache 2.0 License.


.. _Databus: https://github.com/linkedin/databus
.. _PostgreSQL: http://www.postgresql.org/
.. _Raven: https://github.com/getsentry/raven-python
.. _Sentry: https://github.com/getsentry/sentry
.. _SkyTools: http://skytools.projects.pgfoundry.org/
.. _Slony: http://www.slony.info/
.. _`Apache ZooKeeper`: https://zookeeper.apache.org/
.. _`Bottled Water`: https://github.com/confluentinc/bottledwater-pg
.. _`Diamond PgQ Collector`: https://github.com/python-diamond/Diamond/blob/master/src/collectors/pgq/pgq.py
.. _`PostgreSQL docs for transaction iolation`: http://www.postgresql.org/docs/9.4/static/transaction-iso.html
.. _`change data capture`: http://en.wikipedia.org/wiki/Change_data_capture
.. _`example set builder tool`: https://github.com/disqus/pgshovel/blob/master/example/set.py
.. _`logging configuration file`: https://docs.python.org/2/library/logging.config.html#configuration-file-format
.. _`logical decoding`: http://www.postgresql.org/docs/9.4/static/logicaldecoding-explanation.html
.. _`protobuf message defintion`: https://github.com/disqus/pgshovel/blob/master/src/main/protobuf/pgshovel/interfaces/configurations.proto#L33-L41
.. _pgqd: http://skytools.projects.pgfoundry.org/skytools-3.0/doc/pgqd.html
