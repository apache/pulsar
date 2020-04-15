set -e


BINDIR=`dirname "$0"`
CHARTS_HOME=`cd ${BINDIR}/..;pwd`
VALUES_FILE=$1
TLS=${TLS:-"false"}
SYMMETRIC=${SYMMETRIC:-"false"}
FUNCTION=${FUNCTION:-"false"}

source ${CHARTS_HOME}/.ci/helm.sh

# create cluster
ci::create_cluster

# install storage provisioner
ci::install_storage_provisioner

extra_opts=""
if [[ "x${SYMMETRIC}" == "xtrue" ]]; then
    extra_opts="-s"
fi

# install pulsar chart
ci::install_pulsar_chart ${CHARTS_HOME}/${VALUES_FILE} ${extra_opts}

# test producer
ci::test_pulsar_producer

if [[ "x${FUNCTION}" == "xtrue" ]]; then
    # install cert manager
    ci::test_pulsar_function
fi

# delete the cluster
ci::delete_cluster
