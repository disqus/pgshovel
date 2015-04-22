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
	pip install -e .
	pip install -r requirements.test.txt

check:
	pyflakes $$(find $(PYTHON_OUT) $(TESTS_FOLDER) -name \*.py -not -name \*_pb2.py)

test: develop
	py.test $(TESTS_FOLDER)

test-xunit: clean develop
	coverage erase
	py.test --junitxml=$(XUNIT_FILE) --cov pgshovel --cov $(TESTS_FOLDER) --cov-report=xml $(TESTS_FOLDER)
	mv coverage.xml $(COVERAGE_FILE)

deb:
	dpkg-buildpackage

.PHONY: all clean build develop install check test deb
