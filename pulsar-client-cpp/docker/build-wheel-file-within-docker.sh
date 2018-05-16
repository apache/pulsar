#!/bin/bash

cd /pulsar/pulsar-client-cpp

find . -name CMakeCache.txt | xargs rm
find . -name CMakeFiles | xargs rm -rf

cmake . -DPYTHON_INCLUDE_DIR=/opt/python/$PYTHON_SPEC/include/python$PYTHON_VERSION \
        -DPYTHON_LIBRARY=/opt/python/$PYTHON_SPEC/lib \
        -DLINK_STATIC=ON

make clean
make _pulsar

cd python
python setup.py bdist_wheel
