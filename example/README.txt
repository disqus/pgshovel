docker-compose up -d kafka zookeeper pgqd postgres

docker-compose run -e PGHOST=postgres -e PGUSER=postgres --rm postgres psql -c 'CREATE DATABASE example'
docker-compose run -e PGHOST=postgres -e PGUSER=postgres --rm postgres pgbench -i --foreign-keys example

docker-compose run --rm pgshovel cluster initialize
docker-compose run --rm --entrypoint=python pgshovel example/set.py postgres:///example > /tmp/example.pgshovel
docker-compose run --rm pgshovel set create example < /tmp/example.pgshovel

docker-compose run --rm --entrypoint=pgshovel-stream-relay pgshovel example > mutations.log

docker-compose run -e PGHOST=postgres -e PGUSER=postgres --rm postgres pgbench example

docker-compose run -e PGHOST=postgres -e PGUSER=postgres --rm postgres psql -c 'select * from pgq.get_consumer_info();' example
