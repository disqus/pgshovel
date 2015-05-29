Change Log
==========

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
