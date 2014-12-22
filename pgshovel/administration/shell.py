import code

from pgshovel.commands import command


@command
def shell(options, application):
    exports = {
        'application': application,
        'environment': application.environment,
    }
    return code.interact(local=exports)
