def managed(managers, function):
    managers = list(managers)

    def __managed(managers, function):
        if not managers:
            return function()
        else:
            manager = managers.pop(0)
            with manager:
                return __managed(managers, function)

    return __managed(managers, function)
