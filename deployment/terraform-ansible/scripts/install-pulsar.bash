#!/bin/bash

PULSAR_VERSION=$1
PULSAR_ROOT_DIR=/opt/pulsar
PULSAR_DOWNLOAD_URL=http://apache.mirrors.hoobly.com/incubator/pulsar/pulsar-${PULSAR_VERSION}/apache-pulsar-${PULSAR_VERSION}-bin.tar.gz

mkdir -p ${PULSAR_ROOT_DIR}
mkdir -p ${PULSAR_ROOT_DIR}/data/zookeeper

wget ${PULSAR_DOWNLOAD_URL} -O /tmp/pulsar.tgz
cd ${PULSAR_ROOT_DIR}
./conf/pulsar_env.sh

exit 0