pgshovel
########

``pgshovel`` is a `change data capture`_ system for PostgreSQL_, built on top of
`Apache ZooKeeper`_, pgq (part of SkyTools_), and database triggers.

It is inspired by LinkedIn's Databus_ project, and trigger-based asynchronous
replication solutions for PostgreSQL such as Slony_ and Londiste (also part of
SkyTools).

ZooKeeper is used for storing cluster configurations and ensuring that the
configurations remain synchronized with the state of all databases in the
cluster. pgq is used for buffering mutation records until they are collected by
the relay and forwarded to their final destination.

Concepts
========

Cluster
-------

A cluster contains replication sets and multiple databases.

Replication Set
---------------

Replication sets contain a collection of tables that are replicated together.

Where this differs from other replication tooling is that replication sets live
on multiple databases, and there is no distinguishing between primary and
replica databases in a replication set.

Instead, triggers are installed on **all** databases in the replication set,
but only writes performed to the origin database are captured and collected by
the relay. (All other databases will simply report no mutation activity.)

Relay
-----

The relay consumes batches of mutations from all databases within the
replication set, and forwards those mutation batches to their destination.

Usage
=====

Installation Requirements
-------------------------

* A running ZooKeeper cluster.
* A running ``pgq`` ticker for all databases.
* PostgreSQL must have:

  * the SkyTools extension installed,
  * been compiled with ``--with-python``,
  * a non-zero value for the ``max_prepared_transactions`` configuration
    parameter. (This is required to ensure that all databases have been
    configured with the same version of the replication set configuration.)

Creating a Cluster
------------------

::

    pgshovel-initialize-cluster

::

    pgshovel-create-set example < configuration

::

    pgshovel-list-sets

::

    pgshovel-inspect-set example

Running a Relay
---------------

::

    pgshovel-relay example consumer

Updating a Replication Set
--------------------------

::

    pgshovel-upgrade-group example < configuration

Dropping a Replication Set
--------------------------

::

    pgshovel-drop-group example

Configuration
=============

The configuration file search path is (in order of precedence):

#. the path specified by the ``-f`` or ``--configuration-file`` command line option,
#. ``~/.pgshovel`` (the home directory of the user running the command),
#. ``/etc/pgshovel.conf``.

The file ``src/main/python/pgshovel/configuration/pgshovel.conf`` contains the
default configuration and contains comprehensive documentation on the available
options.

Operations
==========

Bootstrapping Consumers
-----------------------

Upgrades
--------

Monitoring
----------

PgQ
~~~

The mutation log (where mutation events are buffered before being forwarded by
the `Relay`_) can be monitored using the `Diamond PgQ Collector`_, or any other
tools designed for monitoring queue consumption and throughput.

PgQ provides many useful data points, including pending (unconsumed) events,
throughput rates, replication lag, and other metrics.

pgshovel-relay
~~~~~~~~~~~~~~

It's is highly recommended to use Raven_ to report application warnings and
errors to a Sentry_ installation by providing a custom `logging configuration
file`_ in your pgshovel `Configuration`_ file.

The ``raven`` Python module is installed by default with the Debian package
installation. The necessary dependencies for reporting can also be installed as
a ``setuptools`` extra with ``pip install pgshovel[sentry]``.

Planned Replica Promotion
-------------------------

Unplanned Replica Promotion
---------------------------

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

In write-heavy environments, it is typically a better choice to choice use
logical decoding (assuming you can run PostgreSQL 9.4), foregoing some
configuration flexibility for increased throughput.

Development
===========

To install the project and all dependencies::

    make develop

To run the test suite::

    make test

Dependency Versioning
---------------------

``pgshovel`` is intended to be used as both a client library, as well as a
standalone application. As such dependencies need to be declared in two places:
``setup.py`` and ``requirements.txt``.

``setup.py`` should include dependencies as version ranges to ensure
compatibility and flexibility with other dependencies when used as a library.

``requirements.txt`` should include dependencies as specific revision tags,
equivalent to the output of ``pip freeze`` in the Debian virtualenv, so that
all standalone deployments always use a consistent collection of dependencies.

Test dependencies should be declared in both the ``tests_require`` section of
``setup.py`` (as flexible ranges), as well as in ``requirements.test.txt`` (as
specific versions.)

.. _Databus: https://github.com/linkedin/databus
.. _PostgreSQL: http://www.postgresql.org/
.. _Raven: https://github.com/getsentry/raven-python
.. _Sentry: https://github.com/getsentry/sentry
.. _SkyTools: http://skytools.projects.pgfoundry.org/
.. _Slony: http://www.slony.info/
.. _`Apache ZooKeeper`: https://zookeeper.apache.org/
.. _`Diamond PgQ Collector`: https://github.com/python-diamond/Diamond/blob/master/src/collectors/pgq/pgq.py
.. _`change data capture`: http://en.wikipedia.org/wiki/Change_data_capture
.. _`logging configuration file`: https://docs.python.org/2/library/logging.config.html#configuration-file-format
.. _`logical decoding`: http://www.postgresql.org/docs/9.4/static/logicaldecoding-explanation.html
