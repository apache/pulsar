#!/bin/bash

PULSAR_VERSION=$1
BROKER_ID=$2
BROKER_IP=$3
WEB_SERVICE_URL=http://${BROKER_IP}:8080
BROKER_SERVICE_URL=pulsar://${BROKER_IP}:6650

PULSAR_ROOT_DIR=/opt/pulsar
PULSAR_DOWNLOAD_URL=http://apache.mirrors.hoobly.com/incubator/pulsar/pulsar-${PULSAR_VERSION}/apache-pulsar-${PULSAR_VERSION}-bin.tar.gz

mkdir -p ${PULSAR_ROOT_DIR}
mkdir -p ${PULSAR_ROOT_DIR}/data/zookeeper

wget ${PULSAR_DOWNLOAD_URL} -O /tmp/pulsar.tgz
cd ${PULSAR_ROOT_DIR}
conf/pulsar_env.sh

if [ "${BROKER_ID}" -eq "0" ]; then
    ${PULSAR_ROOT_DIR}/bin/pulsar initialize-cluster-metadata \
        --cluster local \
        --zookeeper localhost:2181 \
        --global-zookeeper localhost:2181 \
        --web-service-url ${WEB_SERVICE_URL} \
        --broker-service-url ${BROKER_SERVICE_URL}
fi