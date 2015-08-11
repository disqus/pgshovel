JAVA_OUT=src/main/java
PYTHON_OUT=src/main/python
PROTO_FOLDER=src/main/protobuf
TESTS_FOLDER=tests

interfaces: $(JAVA_OUT) $(PYTHON_OUT)
	docker-compose run --rm protoc -I $(PROTO_FOLDER) --java_out=$(JAVA_OUT) --python_out=$(PYTHON_OUT) $$(find $(PROTO_FOLDER) -name "*.proto")

$(JAVA_OUT):
	mkdir -p $(JAVA_OUT)

$(PYTHON_OUT):
	mkdir -p $(PYTHON_OUT)

requirements.txt: setup.py
	python setup.py requirements -s 1 -e all | sed 1d | sort > requirements.txt

build: requirements.txt
	docker-compose build

clean:
	git clean -fdx - $(JAVA_OUT) $(PYTHON_OUT) $(TESTS_FOLDER)

check:
	pyflakes $$(find $(PYTHON_OUT) $(TESTS_FOLDER) -name \*.py -not -name \*_pb2.py)

test: build interfaces
	python ephemeral-cluster.py run --rm --entrypoint=python pgshovel setup.py test

.xunit:
	mkdir -p .xunit

test-xunit: .xunit build interfaces
	python ephemeral-cluster.py run --rm --entrypoint=python pgshovel setup.py test -a "--junit-xml .xunit/results.xml" || test $$? -le 1

.PHONY:
	build \
	check \
	clean \
	interfaces \
	test \
	test-xunit
