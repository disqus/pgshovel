import functools
import pkg_resources


resource_string = functools.partial(pkg_resources.resource_string, 'pgshovel')
resource_filename = functools.partial(pkg_resources.resource_filename, 'pgshovel')
