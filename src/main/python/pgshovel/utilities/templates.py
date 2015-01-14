import functools
import pkg_resources
from string import Template



resource_string = functools.partial(pkg_resources.resource_string, 'pgshovel')
resource_filename = functools.partial(pkg_resources.resource_filename, 'pgshovel')


def template(application, path):
    source = resource_string(path)
    return functools.partial(
        Template(source).substitute,
        application=application.name,
        schema=application.schema,
        queue=application.queue,
    )
