import os
from setuptools import (
    find_packages,
    setup,
)
from setuptools.command.test import test


PACKAGE_DIR = os.path.join('src', 'main', 'python')


class PyTest(test):
    def finalize_options(self):
        test.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest, sys
        errno = pytest.main(self.test_args)
        sys.exit(errno)


setup(
    name='pgshovel',
    install_requires=(
        'kazoo',
        'pgqueue',
        'protobuf',
        'psycopg2',
        'tabulate',
    ),
    entry_points={
        'console_scripts': [
            'pgshovel-create-group = pgshovel.administration:create_group',
            'pgshovel-drop-groups = pgshovel.administration:drop_groups',
            'pgshovel-initialize-cluster = pgshovel.administration:initialize_cluster',
            'pgshovel-inspect-group = pgshovel.administration:inspect_group',
            'pgshovel-list-groups = pgshovel.administration:list_groups',
            'pgshovel-move-groups = pgshovel.administration:move_groups',
            'pgshovel-shell = pgshovel.administration:shell',
            'pgshovel-update-group = pgshovel.administration:update_group',
        ],
    },
    packages=find_packages(PACKAGE_DIR),
    package_dir={
        '': PACKAGE_DIR,
    },
    classifier=['Private :: Do Not Upload'],
    cmdclass = {
        'test': PyTest,
    },
    tests_require=(
        'pytest',
    ),
)
