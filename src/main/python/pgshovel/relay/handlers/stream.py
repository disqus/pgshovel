import functools
import itertools
import threading

import click

from pgshovel.interfaces.streams_pb2 import Message
from pgshovel.relay.entrypoint import entrypoint
from pgshovel.utilities.protobuf import TextCodec


# TODO: This is not very useful with the streaming protocol without wrapper...?


class StreamWriter(object):
    def __init__(self, stream, codec):
        self.stream = stream
        self.codec = codec
        self.__lock = threading.Lock()

    def push(self, messages):
        with self.__lock:
            for encoded in itertools.imap(self.codec.encode, messages):
                self.stream.write(encoded)
                self.stream.write('\n')
            self.stream.flush()


@click.command(
    help="Publishes mutation batches to the specified stream/file.",
)
@click.option(
    '--stream',
    type=click.File('w'),
    default='-',
    help="Path to output file.",
)
@entrypoint
def main(cluster, set, stream):
    return StreamWriter(stream, TextCodec(Message))


__main__ = functools.partial(main, auto_envvar_prefix='PGSHOVEL')

if __name__ == '__main__':
    __main__()
