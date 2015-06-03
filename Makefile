JAVA_OUT=src/main/java
PYTHON_OUT=src/main/python
PROTO_FOLDER=src/main/protobuf
TESTS_FOLDER=tests/

all: build

$(JAVA_OUT):
	mkdir -p $(JAVA_OUT)

$(PYTHON_OUT):
	mkdir -p $(PYTHON_OUT)

clean:
	git clean -fdx - $(JAVA_OUT) $(PYTHON_OUT) $(TESTS_FOLDER)

build: $(JAVA_OUT) $(PYTHON_OUT)
	protoc -I $(PROTO_FOLDER) --java_out=$(JAVA_OUT) --python_out=$(PYTHON_OUT) $$(find $(PROTO_FOLDER) -name "*.proto")

install: build
	pip install .

develop: build
	pip install --upgrade pip setuptools
	pip install -e .[kafka,msgpack]
	pip install -r requirements.test.txt
	make -C vendor all

check:
	pyflakes $$(find $(PYTHON_OUT) $(TESTS_FOLDER) -name \*.py -not -name \*_pb2.py)

test: develop
	KAFKA_PATH=vendor/kafka POSTGRES_PATH=vendor/postgres ZOOKEEPER_PATH=vendor/zookeeper py.test $(TESTS_FOLDER)

test-xunit: clean develop
	coverage erase
	KAFKA_PATH=vendor/kafka POSTGRES_PATH=vendor/postgres ZOOKEEPER_PATH=vendor/zookeeper py.test --junitxml=$(XUNIT_FILE) --cov pgshovel --cov $(TESTS_FOLDER) --cov-report=xml $(TESTS_FOLDER)
	mv coverage.xml $(COVERAGE_FILE)

deb:
	dpkg-buildpackage

images:
	docker build -t pgshovel .
	docker build -t postgres-pgshovel docker/postgres-pgshovel/
	docker build -t python-pgqueue docker/python-pgqueue/

.PHONY:
	all \
	build \
	check \
	clean \
	deb \
	develop \
	images \
	install \
	test \
	test-xunit
