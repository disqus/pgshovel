FROM python:2.7

# The protocol buffer compiler is the least likely thing to change, so it is
# installed first.
RUN apt-get update && \
    apt-get install -y protobuf-compiler  && \
        rm -rf /var/lib/apt/lists/*

# Next, install the requirements (as these are also unlikely to change often.)
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY requirements.txt /usr/src/app/
RUN pip install -r requirements.txt

# Then, copy the the application source into the container, ensuring the proper
# cache behavior for ``make build`` (it only runs if one of the preceding steps
# invalidates the layer cache -- namely, the directory contents have been
# changed.)
COPY . /usr/src/app
RUN make build
RUN pip install -e .

# The default command needs to be run with -it, but is the only
# non-parameterized entry point so the only thing that makes sense to add here
# right now.
CMD ["pgshovel", "shell"]
