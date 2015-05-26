import functools
import inspect
import json
import logging
import logging.config
import optparse
import os
import pkg_resources
import sys
import textwrap
from ConfigParser import (
    NoOptionError,
    SafeConfigParser,
)
from datetime import timedelta

from tabulate import tabulate

from pgshovel.cluster import Cluster
from pgshovel.utilities.templates import (
    resource_filename,
    resource_stream,
)


logger = logging.getLogger(__name__)


CONFIGURATION_PATHS = (
    os.path.expanduser('~/.pgshovel'),
    '/etc/pgshovel.conf',
)


class Option(object):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def add(self, parser):
        parser.add_option(*self.args, **self.kwargs)


def command(function=None, *args, **kwargs):

    def decorator(function, options=(), description=None):
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
            '--version',
            action='store_true',
            help='print version and exit',
        )

        parser.add_option(
            '-c', '--cluster',
            default='default', metavar='NAME',
            help='cluster identifier (%default)',
        )

        parser.add_option(
            '-f', '--configuration-file', metavar='PATH',
            help='path to configuration file (defaults to %s)' % ', '.join(CONFIGURATION_PATHS),
        )

        for option in options:
            option.add(parser)

        @functools.wraps(function)
        def wrapper():
            options, arguments = parser.parse_args()

            if options.version:
                print pkg_resources.get_distribution("pgshovel").version
                sys.exit(0)

            configuration = SafeConfigParser(defaults={
                'cluster': options.cluster,
            })
            configuration.readfp(resource_stream('configuration/pgshovel.conf'))
            configuration.read((options.configuration_file,) if options.configuration_file is not None else CONFIGURATION_PATHS)

            try:
                logging_configuration = configuration.get('logging', 'configuration')
            except NoOptionError:
                logging_configuration = resource_filename('configuration/logging.conf')

            if logging_configuration:
                logging.config.fileConfig(logging_configuration)

            try:
                cluster = Cluster(options.cluster, configuration)

                try:
                    return function(options, cluster, *arguments)
                except TypeError as e:
                    if str(e).startswith('%s() takes ' % function.__name__):
                        parser.print_usage(sys.stderr)
                        sys.exit(1)
                    else:
                        raise
            except Exception as error:
                logger.exception(error)
                sys.exit(1)

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
    '--format', metavar='FORMATTER',
    choices=formatters.keys(),
    default='table',
    help='output formatter to use (one of: %s)' % ', '.join(sorted(formatters)),
)
