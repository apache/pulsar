#!/bin/bash

ROOT_DIR=$(git rev-parse --show-toplevel)
INPUT=$ROOT_DIR/pulsar-client-cpp/python/pulsar.py
DESTINATION=$ROOT_DIR/site/api/python

pdoc $INPUT \
  --html \
  --html-dir $DESTINATION
mv $DESTINATION/pulsar.m.html $DESTINATION/index.html
