#!/bin/bash

set -e

BUILD_IMAGE_NAME="${BUILD_IMAGE_NAME:-apachepulsar/pulsar-build}"

ROOT_DIR=$(git rev-parse --show-toplevel)
cd $ROOT_DIR

PYTHON_VERSIONS=(
   '2.7 cp27-cp27mu'
   '2.7 cp27-cp27m'
   '3.3 cp33-cp33m'
   '3.4 cp34-cp34m'
   '3.5 cp35-cp35m'
   '3.6 cp36-cp36m'
)

for line in "${PYTHON_VERSIONS[@]}"; do
    read -r -a PY <<< "$line"
    PYTHON_VERSION=${PY[0]}
    PYTHON_SPEC=${PY[1]}
    echo "--------- Build Python wheel for $PYTHON_VERSION -- $PYTHON_SPEC"

    IMAGE_NAME=$BUILD_IMAGE_NAME:manylinux-$PYTHON_SPEC

    echo "Using image: $IMAGE_NAME"
    docker run -i -v $PWD:/pulsar $IMAGE_NAME /build.sh
done
