import threading


class Handler(object):
    """
    Handler objects receive updates from consumer threads.

    Handler objects are not thread safe by themselves, but their underlying
    implementations may be. If the `__call__` implementation requires
    synchronized access to an underlying resource (for instance, a file-like
    object or socket), the handler **must** provide it's own synchronization
    mechanisms.

    Any exceptions that are thrown as part of the `__call__` method cause the
    batch to be considered failed and retried until the method successfully
    executes. Intentionally supressing exceptions can be used for consumers
    where data integrity is less important (such as ephemeral publish/subscribe
    streams) and timeliness of delivery is more important.
    """
    def __call__(self, group, configuration, events):
        raise NotImplementedError

    @classmethod
    def build(cls, application, *args):
        raise NotImplementedError


class StreamHandler(Handler):
    """
    Writes event payloads to a stream.
    """
    def __init__(self, stream, template):
        self.stream = stream
        self.template = template

        self.__lock = threading.Lock()

    def __call__(self, group, configuration, events):
        with self.__lock:
            write = self.stream.write
            template = self.template
            for event in events:
                write(template.format(group=group, configuration=configuration, event=event))
            self.stream.flush()

    @classmethod
    def build(cls, application, path='/dev/stdout', template='{group} {event.data}\n'):
        return cls(open(path, 'w'), template)
