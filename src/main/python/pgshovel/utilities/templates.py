import atexit
import functools
import pkg_resources

# if this module is imported, you're probably going to need to clean up afterwards
from pkg_resources import cleanup_resources

atexit.register(cleanup_resources)


resource_string = functools.partial(pkg_resources.resource_string, 'pgshovel')
resource_filename = functools.partial(pkg_resources.resource_filename, 'pgshovel')
resource_stream = functools.partial(pkg_resources.resource_stream, 'pgshovel')
