#!/usr/bin/env bash -ex
pgshovel-initialize-cluster -a pgbench

for group in accounts branches tellers
do
    cat groups/$group | python configure.py $@ | pgshovel-create-group -a pgbench $group
done
