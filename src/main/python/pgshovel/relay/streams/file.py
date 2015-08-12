import itertools
import threading

from pgshovel.interfaces.streams_pb2 import Message
from pgshovel.utilities.protobuf import TextCodec


class FileWriter(object):
    def __init__(self, stream):
        self.stream = stream
        self.codec = TextCodec(Message)
        self.__lock = threading.Lock()

    def push(self, messages):
        with self.__lock:
            for encoded in itertools.imap(self.codec.encode, messages):
                self.stream.write(encoded)
                self.stream.write('\n')
            self.stream.flush()

    @classmethod
    def configure(cls, cluster, set, path):
        return cls(open(path, 'a'))
