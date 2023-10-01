.PHONY: all build

ifndef NODOCKER
SHELL := BASH_ENV=.rc /bin/bash --noprofile
endif

all: build test

build: ; mvn clean -pl pulsar-io/rabbitmq install -DskipTests

test: ; mvn clean -pl pulsar-io/rabbitmq test

