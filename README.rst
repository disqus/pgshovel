pgshovel
########

**This project is still under active development and should not yet be
considered stable enough for production use as a critical infrastructure
component.**

``pgshovel`` is a `change data capture`_ system for PostgreSQL_, built on top of
`Apache ZooKeeper`_, PgQ (part of SkyTools_), and database triggers.

It is inspired by LinkedIn's Databus_ project, and trigger-based asynchronous
replication solutions for PostgreSQL such as Slony_ and Londiste (also part of
SkyTools).

ZooKeeper is used for storing cluster configurations and ensuring that the
configurations remain synchronized with the state of all databases in the
cluster. PgQ is used for buffering mutation records until they are collected by
the relay and forwarded to their final destination.

Concepts
========

Cluster
-------

A cluster contains replication sets.

Replication Set
---------------

Replication sets contain a collection of tables that are replicated together.

Relay
-----

The relay consumes batches of mutations for the replication set and forwards
those mutation batches to their destination.

Usage
=====

Installation Requirements
-------------------------

* A running ZooKeeper cluster.
* A running ``PgQ`` ticker (such as pgqd_) for all databases.
* PostgreSQL must have:

  * the SkyTools extension installed,
  * been compiled with ``--with-python``,
  * a non-zero value for the ``max_prepared_transactions`` configuration
    parameter. (This is required to ensure that all databases have been
    configured with the same version of the replication set configuration.)

Creating a Cluster
------------------

.. todo:: Rewrite this as a tutorial using pgbench after the replication worker is done.

::

    pgshovel cluster initialize

::

    pgshovel set create example /path/to/configuration.pgshovel

::

    pgshovel set list

::

    pgshovel set inspect example

Running a Relay
---------------

::

    pgshovel-kafka-relay example

Updating a Replication Set
--------------------------

::

    pgshovel set upgrade /path/to/configuration.pgshovel

Dropping a Replication Set
--------------------------

::

    pgshovel set drop example

Configuration
=============

The execution environment can be controlled in two ways: command line flags and
environment variables. There is a small set of configuration parameters that
are available on every command (``pgshovel --help``), while the remainder are
distinct on a command basis.

All command options can also be defined as environment variables. To translate
an option to an environment variable, prefix the option name with
``PGSHOVEL_``, uppercase the label, and convert all dashes to underscores. For
example, ``--zookeeper-hosts`` becomes ``PGSHOVEL_ZOOKEEPER_HOSTS``.

Operations
==========

Bootstrapping Consumers
-----------------------

.. todo:: Rewrite this as part of the tutorial using pgbench after the replication worker is done.

Upgrades
--------

.. todo:: Fix node watch issue in relay, update this to reflect automatic restart.

Monitoring
----------

PgQ
~~~

The mutation log (where mutation events are buffered before being forwarded by
the Relay) can be monitored using the `Diamond PgQ Collector`_, or any other
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
.. _`change data capture`: http://en.wikipedia.org/wiki/Change_data_capture
.. _`logging configuration file`: https://docs.python.org/2/library/logging.config.html#configuration-file-format
.. _`logical decoding`: http://www.postgresql.org/docs/9.4/static/logicaldecoding-explanation.html
.. _pgqd: http://skytools.projects.pgfoundry.org/skytools-3.0/doc/pgqd.html
