Change Log
==========

Version 0.2.4
-------------

- Upgrades ``futures`` dependency to all 3.0 compatible versions.
- Fixes issue with Kafka producer construction in test suite.

Version 0.2.3
-------------

- Adds the ability to monitor all columns in a table by omitting a column list.
- Adds support for bidirectional JSON encoding when using the JSON codec.
  (This enabled by default. To prevent the codec from adding type annotations,
  ``pgshovel.codecs:jsonlite`` can be used.)

Version 0.2.2
------------

- Changes command line interface to a single entry point for all management
  commands (``pgshovel``), and separate entry points for relays
  (``pgshovel-stream-relay``, ``pgshovel-kafka-relay``.)
- Removes Debian support, in favor of Docker.
- Removes configuration file support in favor of command line flags and
  environment variables for easier container deployment.

Features
~~~~~~~~

- Add JSON codec (although right now, it only acts as an encoder.)
- Adds support for enviromnment variables for configuration.


Version 0.2.1.1
----------------

Bug Fixes
~~~~~~~~~

- Adds validation to ensure primary key columns are set on table
  configurations.
- Replaces `SIGINFO` with `SIGUSR1` (for relay diagnostics) for Linux
  compatibility. (`SIGINFO` is still handled if available for conveience on
  BSD-based systems.)

Version 0.2.1
-------------

Features
~~~~~~~~

- Adds testing utilities for generating mock batches and events.

Improvements
~~~~~~~~~~~~

- Adds deep attribute access to dynamic loading utility.
