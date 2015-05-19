class FormattedSequence(object):
    def __init__(self, value):
        self.__value = value

    def __str__(self):
        if self.__value:
            return ', '.join(map(str, sorted(self.__value)))
        else:
            return '(empty)'
