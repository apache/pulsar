#!/bin/bash

# Create all the Docker images for variations of Python versions

set -e

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
    echo "--------- Build Docker image for $PYTHON_VERSION -- $PYTHON_SPEC"

    IMAGE_NAME=pulsar-build:manylinux-$PYTHON_SPEC

    docker build -t $IMAGE_NAME . \
            --build-arg PYTHON_VERSION=$PYTHON_VERSION \
            --build-arg PYTHON_SPEC=$PYTHON_SPEC

    echo "==== Successfully built image $IMAGE_NAME"
done
