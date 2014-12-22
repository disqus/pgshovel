from setuptools import setup


setup(
    name='pgshovel',
    install_requires=(
        'kazoo',
        'pgqueue',
        'psycopg2',
        'tabulate',
    ),
    entry_points={
        'console_scripts': [
            'pgshovel-add-database = pgshovel.administration.databases:add_database',
            'pgshovel-consumer = pgshovel.consumer:consume',
            'pgshovel-create-group = pgshovel.administration.groups:create_group',
            'pgshovel-drop-group = pgshovel.administration.groups:drop_group',
            'pgshovel-initialize-cluster = pgshovel.administration.databases:initialize_cluster',
            'pgshovel-list-databases = pgshovel.administration.databases:list_databases',
            'pgshovel-list-groups = pgshovel.administration.groups:list_groups',
            'pgshovel-move-group = pgshovel.administration.groups:move_group',
            'pgshovel-shell = pgshovel.administration.shell:shell',
        ],
    },
    classifier=['Private :: Do Not Upload'],
)
