#!/usr/bin/env python
import itertools
import subprocess
import sys
import uuid


def get_images_for_project(project):
    """
    Returns a set of image names associated with a project label.
    """
    p = subprocess.Popen(['docker', 'images'], stdout=subprocess.PIPE)
    images = set()
    while p.returncode is None:
        out, err = p.communicate()
        for line in itertools.ifilter(None, out.splitlines()):
            bits = line.split()
            if bits[0].startswith('{0}_'.format(project)):
                images.add(bits[0])

    if p.returncode != 0:
        raise Exception('Error while retrieving images!')

    return images


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


subprocess.check_call(['docker-compose', '-p', project, 'build'])
try:
    subprocess.check_call(['docker-compose', '-p', project] + sys.argv[1:])
except subprocess.CalledProcessError as error:
    raise SystemExit(error.returncode)
finally:
    sys.stderr.write('\nCleaning up containers for ephemeral cluster ({0})...\n'.format(project))
    subprocess.check_call(['docker-compose', '-p', project, 'stop'])
    subprocess.check_call(['docker-compose', '-p', project, 'rm', '-f', '-v'])

    sys.stderr.write('\nCleaning up images for ephemeral cluster ({0})...\n'.format(project))
    subprocess.check_call(['docker', 'rmi'] + list(get_images_for_project(project)))
