#!/bin/bash

# Create all the Docker images for variations of Python versions

set -e

DOCKER_ORG=apachepulsar

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
    
    IMAGE_NAME=pulsar-build:manylinux-$PYTHON_SPEC
    FULL_NAME=$DOCKER_ORG/$IMAGE_NAME

    echo "IMAGE_NAME: $IMAGE_NAME"
    echo "FULL_NAME: $FULL_NAME"
    docker tag $IMAGE_NAME $FULL_NAME
    docker push $FULL_NAME

    echo "==== Successfully pushed image $FULL_NAME"
done
