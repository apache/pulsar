#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -e


BINDIR=`dirname "$0"`
PULSAR_HOME=`cd ${BINDIR}/..;pwd`
VALUES_FILE=$1
TLS=${TLS:-"false"}
SYMMETRIC=${SYMMETRIC:-"false"}
FUNCTION=${FUNCTION:-"false"}

source ${PULSAR_HOME}/.ci/helm.sh

CHARTS_HOME="${PULSAR_HOME}/deployment/kubernetes/helm"

# create cluster
ci::create_cluster

# install storage provisioner
ci::install_storage_provisioner

extra_opts=""
if [[ "x${SYMMETRIC}" == "xtrue" ]]; then
    extra_opts="-s"
fi

# install pulsar chart
ci::install_pulsar_chart ${PULSAR_HOME}/${VALUES_FILE} ${extra_opts}

# test producer
ci::test_pulsar_producer

if [[ "x${FUNCTION}" == "xtrue" ]]; then
    # install cert manager
    ci::test_pulsar_function
fi

# delete the cluster
ci::delete_cluster
