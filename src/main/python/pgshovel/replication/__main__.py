import functools
import yaml

import click

from pgshovel.utilities.components import configure
from pgshovel.utilities.commands import entrypoint


@click.command()
@click.argument('configuration', type=click.File('rb'))  # TODO: YamlFile
@click.argument('set')
@entrypoint
def main(cluster, configuration, set):
    configuration = yaml.load(configuration)
    with cluster:
        target = configure(configuration['target'])(cluster, set)
        loader = configure(configuration['loader'])(cluster, set)
        stream = configure(configuration['stream'])(cluster, set)
        target.run(loader, stream)


__main__ = functools.partial(
    main,
    auto_envvar_prefix='PGSHOVEL',
)


if __name__ == '__main__':
    __main__()
