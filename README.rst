PostgreSQL Transaction Shovel
=============================

Installation
------------

* The PGQ extension must be available on the databases that will contain
  capture groups, and the ``pgqd`` ticker should be running for those databases.
* Prepared transactions (two-phase commit) must be enabled by setting the
  PostgreSQL ``max_prepared_transactions`` configuration parameter to a value
  greater than zero. (Since prepared transactions are only opened during
  cluster modification, this does not need to be a very large value. There will
  be rarely more than one prepared transaction running on a single database at
  a time, with the exception of concurrent cluster modifications.)
* The ``plpythonu`` langauge must be available, and have the ``json`` module.

Command Overview
----------------

All commands take a ``--zookeeper-hosts`` option that can be used to specify
the ZooKeeper connection path.

For detailed usage notes, pass the ``-h`` or ``--help`` flag to the command.

Cluster Initialization
~~~~~~~~~~~~~~~~~~~~~~

::

    pgshovel-initialize-cluster

Adding a Database
~~~~~~~~~~~~~~~~~

::

    pgshovel-add-database $NAME < configuration.json

Listing Databases
~~~~~~~~~~~~~~~~~

::

    pgshovel-list-databases

Creating a Capture Group
~~~~~~~~~~~~~~~~~~~~~~~~

::

    pgshovel-create-group $NAME $DATABASE $TABLE ...

Listing Capture Groups
~~~~~~~~~~~~~~~~~~~~~~

::

    pgshovel-list-groups

Moving a Capture Group between Databases
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    pgshovel-move-group $NAME $DATABASE

Dropping a Capture Group
~~~~~~~~~~~~~~~~~~~~~~~~

::

    pgshovel-drop-group $NAME

Development
-----------

::

    make develop
