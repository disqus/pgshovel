import traceback


def chained(message, cls=Exception, *args, **kwargs):
    message = '%s\n\nOriginal Exception: %s' % (message, traceback.format_exc())
    raise cls(message, *args, **kwargs)
