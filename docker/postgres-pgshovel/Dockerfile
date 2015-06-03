FROM postgres:9.3

RUN apt-get update && \
    apt-get install -y postgresql-9.3-pgq3 postgresql-plpython-9.3 && \
        rm -rf /var/lib/apt/lists/*

CMD ["postgres", "-c", "max_prepared_transactions=10"]
