#!/bin/bash

ROOT_DIR=$(git rev-parse --show-toplevel)
DOXYGEN=$ROOT_DIR/site/scripts/doxygen/build/bin/doxygen
TMP_DIR=$(mktemp -d)

(
  cd $ROOT_DIR/pulsar-client-cpp
  $DOXYGEN Doxyfile
)

mv api/cpp $TMP_DIR
mv $TMP_DIR/cpp/html api/cpp
