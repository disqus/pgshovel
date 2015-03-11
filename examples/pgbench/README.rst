Basic Testing with pgbench
==========================

Create a database.

::

    createdb $DATABASE

Install the `pgbench` schema.

::

    pgbench -s 10 -i $DATABASE

Configure the pgshovel cluster.

::

    ./setup.sh postgresql:///$DATABASE

Make sure the PGQ ticker is running. (Requires ``pgqueue`` module, which can be
installed via ``pip``: ``pip install pgqueue``.)

::

    python -m pgqueue dbname=$DATABASE

Start a consumer.

::

    pgshovel-consumer -a pgbench

Run the `pgbench` application.

::

    pgbench -t 10000 -c 10 $DATABASE

Watch the PGQ consumer statistics:

::

    watch -n0.1 'psql -c "select * from pgq.get_consumer_info();" $DATABASE'
