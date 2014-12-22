import collections
import json


class Codec(object):
    def load(self, fp):
        raise NotImplementedError

    def loads(self, value):
        raise NotImplementedError

    def dump(self, value, fp):
        raise NotImplementedError

    def dumps(self, value):
        raise NotImplementedError


class JsonCodec(Codec):
    def load(self, fp):
        return self.loads(fp.read())

    def loads(self, value):
        return json.loads(value)

    def dump(self, value, fp):
        return fp.write(self.dumps(value))

    def dumps(self, value):
        return json.dumps(value, default=self.__default)

    def __default(self, value):
        if isinstance(value, collections.Mapping):
            return dict(value)

        raise TypeError('Could not encode value: %r' % (value,))
