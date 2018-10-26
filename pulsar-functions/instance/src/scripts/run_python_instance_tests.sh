#!/usr/bin/env bash

# Make sure dependencies are installed
pip install mock --user
pip install protobuf --user

CUR_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
PULSAR_HOME=$CUR_DIR/../../../../

# run instance tests
PULSAR_HOME=${PULSAR_HOME} PYTHONPATH=${PULSAR_HOME}/pulsar-functions/instance/target/python-instance python -m unittest discover -v ${PULSAR_HOME}/pulsar-functions/instance/target/python-instance/tests