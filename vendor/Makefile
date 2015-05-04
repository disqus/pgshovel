POSTGRES_MIRROR = https://ftp.postgresql.org/pub/source
APACHE_MIRROR = http://mirrors.sonic.net/apache

POSTGRES_VERSION := 9.3.0
POSTGRES_FULL = postgresql-$(POSTGRES_VERSION)
POSTGRES_URL = $(POSTGRES_MIRROR)/v$(POSTGRES_VERSION)/$(POSTGRES_FULL).tar.gz
POSTGRES_BUILD_DIR = $(CURDIR)/$(POSTGRES_FULL)/build
POSTGRES_SRC_TGZ = $(notdir $(POSTGRES_URL))

ZOOKEEPER_VERSION := 3.4.6
ZOOKEEPER_FULL = zookeeper-$(ZOOKEEPER_VERSION)
ZOOKEEPER_URL = $(APACHE_MIRROR)/zookeeper/$(ZOOKEEPER_FULL)/$(ZOOKEEPER_FULL).tar.gz
ZOOKEEPER_TGZ = $(notdir $(ZOOKEEPER_URL))

KAFKA_VERSION := 0.8.1.1
KAFKA_FULL = kafka-$(KAFKA_VERSION)
KAFKA_FILENAME = $(KAFKA_FULL)-src
KAFKA_URL = $(APACHE_MIRROR)/kafka/$(KAFKA_VERSION)/$(KAFKA_FILENAME).tgz
KAFKA_SRC_TGZ = $(notdir $(KAFKA_URL))

all: \
	kafka \
	postgres \
	zookeeper

$(POSTGRES_FULL):
	curl -O $(POSTGRES_URL)
	tar xzf $(POSTGRES_SRC_TGZ)
	rm $(POSTGRES_SRC_TGZ)

$(POSTGRES_BUILD_DIR):
	mkdir -p POSTGRES_BUILD_DIR

postgres: $(POSTGRES_FULL)
	cd $(POSTGRES_FULL) && ./configure --prefix=$(POSTGRES_BUILD_DIR) --with-python && make && make install check clean
	rm -f postgres && ln -fs $(POSTGRES_FULL) postgres

$(ZOOKEEPER_FULL):
	curl -O $(ZOOKEEPER_URL)
	tar xvf $(ZOOKEEPER_TGZ)
	rm $(ZOOKEEPER_TGZ)

$(ZOOKEEPER_FULL)/conf/zoo.cfg: $(ZOOKEEPER_FULL)
	cd $(ZOOKEEPER_FULL)/conf && ln -fs zoo_sample.cfg zoo.cfg

zookeeper: $(ZOOKEEPER_FULL) $(ZOOKEEPER_FULL)/conf/zoo.cfg
	rm -f zookeeper && ln -fs $(ZOOKEEPER_FULL) zookeeper
	cp templates/zookeeper.properties.template zookeeper/conf/

$(KAFKA_FULL):
	curl -O $(KAFKA_URL)
	tar xzf $(KAFKA_SRC_TGZ)
	mv $(KAFKA_FILENAME) $(KAFKA_FULL)
	rm $(KAFKA_SRC_TGZ)

kafka: $(KAFKA_FULL)
	cd $(KAFKA_FULL) && ./gradlew jar
	rm -f kafka && ln -fs $(KAFKA_FULL) kafka
	cp templates/kafka.properties.template kafka/config/
	cp templates/log4j.properties.template kafka/config/test-log4j.properties
