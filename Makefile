JAVA_OUT=src/main/java
PYTHON_OUT=src/main/python
PROTO_FOLDER=src/main/protobuf

all: build

$(JAVA_OUT):
	mkdir -p $(JAVA_OUT)

$(PYTHON_OUT):
	mkdir -p $(PYTHON_OUT)

clean:
	git clean -fdx - $(JAVA_OUT) $(PYTHON_OUT)

build: clean $(JAVA_OUT) $(PYTHON_OUT)
	protoc -I $(PROTO_FOLDER) --java_out=$(JAVA_OUT) --python_out=$(PYTHON_OUT) $$(find $(PROTO_FOLDER) -name *.proto)

install: build
	pip install .

develop: build
	pip install -e .

check:
	pyflakes $$(find $(PYTHON_OUT) -name \*.py -not -name \*_pb2.py)

.PHONY: all clean build develop install check
