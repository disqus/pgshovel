from google.protobuf.text_format import Merge


class BinaryCodec(object):
    def __init__(self, cls):
        self.cls = cls

    def encode(self, message):
        assert isinstance(message, self.cls)
        return message.SerializeToString()

    def decode(self, payload):
        return self.cls.FromString(payload)


class TextCodec(object):
    def __init__(self, cls):
        self.cls = cls

    def encode(self, message):
        return str(message)

    def decode(self, payload):
        m = self.cls()
        Merge(payload, m)
        # TODO: Replace this with a better validation routine.
        m.SerializeToString()
        return m
