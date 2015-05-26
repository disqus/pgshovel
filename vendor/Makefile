APACHE_MIRROR = http://mirrors.sonic.net/apache

all: \
	kafka \
	postgres \
	skytools \
	zookeeper

# PostgreSQL

POSTGRES_MIRROR = https://ftp.postgresql.org/pub/source
POSTGRES_VERSION := 9.3.0
POSTGRES_FULL = postgresql-$(POSTGRES_VERSION)
POSTGRES_URL = $(POSTGRES_MIRROR)/v$(POSTGRES_VERSION)/$(POSTGRES_FULL).tar.gz
POSTGRES_SRC_TGZ = $(notdir $(POSTGRES_URL))

$(POSTGRES_FULL)-src:
	curl -O $(POSTGRES_URL)
	tar xzf $(POSTGRES_SRC_TGZ)
	rm $(POSTGRES_SRC_TGZ)
	mv $(POSTGRES_FULL) $(POSTGRES_FULL)-src

$(POSTGRES_FULL): $(POSTGRES_FULL)-src
	cd $(POSTGRES_FULL)-src && \
		./configure --prefix=$(shell pwd)/$(POSTGRES_FULL) --with-python && \
		make all check install clean

postgres: $(POSTGRES_FULL)
	rm -f postgres && ln -fs $(POSTGRES_FULL) postgres


# ZooKeeper

ZOOKEEPER_VERSION := 3.4.6
ZOOKEEPER_FULL = zookeeper-$(ZOOKEEPER_VERSION)
ZOOKEEPER_URL = $(APACHE_MIRROR)/zookeeper/$(ZOOKEEPER_FULL)/$(ZOOKEEPER_FULL).tar.gz
ZOOKEEPER_TGZ = $(notdir $(ZOOKEEPER_URL))

$(ZOOKEEPER_FULL):
	curl -O $(ZOOKEEPER_URL)
	tar xvf $(ZOOKEEPER_TGZ)
	rm $(ZOOKEEPER_TGZ)

$(ZOOKEEPER_FULL)/conf/zoo.cfg: $(ZOOKEEPER_FULL)
	cd $(ZOOKEEPER_FULL)/conf && ln -fs zoo_sample.cfg zoo.cfg

zookeeper: $(ZOOKEEPER_FULL) $(ZOOKEEPER_FULL)/conf/zoo.cfg
	rm -f zookeeper && ln -fs $(ZOOKEEPER_FULL) zookeeper
	cp templates/zookeeper.properties.template zookeeper/conf/


# Kafka

KAFKA_VERSION := 0.8.1.1
KAFKA_FULL = kafka-$(KAFKA_VERSION)
KAFKA_FILENAME = $(KAFKA_FULL)-src
KAFKA_URL = $(APACHE_MIRROR)/kafka/$(KAFKA_VERSION)/$(KAFKA_FILENAME).tgz
KAFKA_SRC_TGZ = $(notdir $(KAFKA_URL))

$(KAFKA_FULL): $(ZOOKEEPER_FULL)
	curl -O $(KAFKA_URL)
	tar xzf $(KAFKA_SRC_TGZ)
	mv $(KAFKA_FILENAME) $(KAFKA_FULL)
	rm $(KAFKA_SRC_TGZ)

kafka: $(KAFKA_FULL)
	cd $(KAFKA_FULL) && ./gradlew jar
	rm -f kafka && ln -fs $(KAFKA_FULL) kafka
	cp templates/log4j.properties.template kafka/config/test-log4j.properties


# SkyTools (PgQ)

SKYTOOLS_VERSION := 3.2
SKYTOOLS_URL := http://pgfoundry.org/frs/download.php/3622/skytools-$(SKYTOOLS_VERSION).tar.gz
SKYTOOLS_TGZ := $(notdir $(SKYTOOLS_URL))
SKYTOOLS_FULL := skytools-$(SKYTOOLS_VERSION)

$(SKYTOOLS_FULL)-src:
	curl -LO $(SKYTOOLS_URL)
	tar xvf $(SKYTOOLS_TGZ)
	rm $(SKYTOOLS_TGZ)
	mv $(SKYTOOLS_FULL) $(SKYTOOLS_FULL)-src

$(SKYTOOLS_FULL): $(SKYTOOLS_FULL)-src $(POSTGRES_FULL)
	cd $(SKYTOOLS_FULL)-src && \
		./configure --prefix=$(shell pwd)/$(SKYTOOLS_FULL) --with-pgconfig=$(shell pwd)/$(POSTGRES_FULL)/bin/pg_config && \
		make all test install clean
	touch $(SKYTOOLS_FULL)  # prevent $(SKYTOOLS_FULL)-src being "newer" than $(SKYTOOLS_FULL) if build succeeded

skytools: $(SKYTOOLS_FULL)
	rm -f skytools && ln -fs $(SKYTOOLS_FULL) skytools
