PULSAR_VERSION=${PULSAR_VERSION:-"2.1.0-incubating"}

mkdir -p /opt
cd /opt
wget http://archive.apache.org/dist/incubator/pulsar/pulsar-${PULSAR_VERSION}/apache-pulsar-${PULSAR_VERSION}-bin.tar.gz
mv apache-pulsar-${PULSAR_VERSION} pulsar
