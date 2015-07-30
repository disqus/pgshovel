#!/usr/bin/env python
import subprocess
import sys
import uuid


usage = """\
Run a command using a temporary docker-compose cluster, removing all containers \
and associated volumes after command completion (regardless of success or \
failure.)

Generally, this would be used with the ``run`` command to provide a clean room \
testing environment.
"""


if not sys.argv[1:]:
    sys.stderr.write(usage)
    sys.exit(1)


project = uuid.uuid1().hex
sys.stderr.write('Setting up ephemeral cluster ({0})...\n'.format(project))

try:
    subprocess.check_call(['docker-compose', '-p', project] + sys.argv[1:])
except subprocess.CalledProcessError as error:
    raise SystemExit(error.returncode)
finally:
    sys.stderr.write('\nCleaning up ephemeral cluster ({0})...\n'.format(project))
    subprocess.check_call(['docker-compose', '-p', project, 'stop'])
    subprocess.check_call(['docker-compose', '-p', project, 'rm', '-f', '-v'])
