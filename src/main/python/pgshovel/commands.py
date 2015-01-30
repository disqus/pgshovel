import atexit
import functools
import inspect
import json
import optparse
import sys
import textwrap
from datetime import timedelta

import logging.config
from kazoo.client import KazooClient
from pkg_resources import cleanup_resources
from tabulate import tabulate

from pgshovel.application import (
    Application,
    Environment,
)
from pgshovel.interfaces.application_pb2 import (
    ApplicationConfiguration,
    EnvironmentConfiguration,
)
from pgshovel.utilities.templates import resource_filename


class Option(object):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def add(self, parser):
        parser.add_option(*self.args, **self.kwargs)


def command(function=None, *args, **kwargs):

    def decorator(function, options=(), description=None, start=True):
        argspec = inspect.getargspec(function)

        arguments = ' '.join(argspec.args[2:])
        if argspec.varargs:
            arguments = '%s [%s ...]' % (arguments, argspec.varargs,)

        if description:
            description = textwrap.dedent(description)

        parser = optparse.OptionParser(
            usage='%%prog [options] %s' % arguments,
            description=description,
        )

        parser.add_option(
            '-a', '--application',
            default='default', metavar='NAME',
            help='application identifier (%default)',
        )

        parser.add_option(
            '--logging-configuration',
            metavar='FILE',
            help='logging configuration file',
        )

        parser.add_option(
            '--zookeeper-hosts',
            default='localhost:2181', metavar='HOSTS',
            help='ZooKeeper connection string (%default)',
        )

        for option in options:
            option.add(parser)

        @functools.wraps(function)
        def wrapper():
            options, arguments = parser.parse_args()

            if options.logging_configuration:
                logging_configuration = options.logging_configuration
            else:
                logging_configuration = resource_filename('logging.conf')
                atexit.register(cleanup_resources)

            logging.config.fileConfig(logging_configuration)

            environment = Environment(
                EnvironmentConfiguration(
                    zookeeper=EnvironmentConfiguration.ZooKeeperConfiguration(
                        hosts=options.zookeeper_hosts,
                    ),
                ),
            )

            application = Application(
                environment,
                ApplicationConfiguration(
                    name=options.application,
                ),
            )

            if start:
                environment.zookeeper.start()
                application.start()

            try:
                try:
                    return function(options, application, *arguments)
                except TypeError as e:
                    if str(e).startswith('%s() takes ' % function.__name__):
                        parser.print_usage(sys.stderr)
                        sys.exit(1)
                    else:
                        raise
            finally:
                # TODO: Find more graceful way to shut down the entire environment
                # and close all connections, rather than just ZooKeeper.
                environment.zookeeper.stop()

        return wrapper

    if function and (not args and not kwargs):
        return decorator(function)
    else:
        return functools.partial(decorator, *args, **kwargs)


def _default_json(value):
    if isinstance(value, timedelta):
        return value.total_seconds()
    else:
        raise TypeError('%r is not JSON serializable')


formatters = {
    'json': lambda rows, headers: json.dumps([dict(zip(headers, row)) for row in rows], indent=2, default=_default_json),
    'table': tabulate,
}


FormatOption = Option(
    '-f', '--format', metavar='FORMATTER',
    choices=formatters.keys(),
    default='table',
    help='output formatter to use (one of: %s)' % ', '.join(sorted(formatters)),
)
