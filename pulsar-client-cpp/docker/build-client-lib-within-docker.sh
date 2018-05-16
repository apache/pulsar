#!/bin/bash

set -e -x

cd /pulsar/pulsar-client-cpp

find . -name CMakeCache.txt | xargs rm
find . -name CMakeFiles | xargs rm -rf
rm lib/*.pb.*

cmake . -DBUILD_TESTS=OFF -DLINK_STATIC=ON \
        -DPYTHON_INCLUDE_DIR=/opt/python/$PYTHON_SPEC/include/python$PYTHON_VERSION \
        -DPYTHON_LIBRARY=/opt/python/$PYTHON_SPEC/lib \

make pulsarShared pulsarStatic -j4
