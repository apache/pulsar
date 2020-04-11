#!/usr/bin/env bash
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

CHART_HOME=$(unset CDPATH && cd $(dirname "${BASH_SOURCE[0]}")/../.. && pwd)
cd ${CHART_HOME}

usage() {
    cat <<EOF
This script is used to cleanup the credentials for a given pulsar helm release. 
Options:
       -h,--help                        prints the usage message
       -n,--namespace                   the k8s namespace to install the pulsar helm chart
       -k,--release                     the pulsar helm release name
       -d,--delete-namespace            flag to delete k8s namespace.
Usage:
    $0 --namespace pulsar --release pulsar-release
EOF
}


while [[ $# -gt 0 ]]
do
key="$1"

delete_namespace=false

case $key in
    -n|--namespace)
    namespace="$2"
    shift
    shift
    ;;
    -d|--delete-namespace)
    delete_namespace=true
    shift
    ;;
    -k|--release)
    release="$2"
    shift
    shift
    ;;
    -h|--help)
    usage
    exit 0
    ;;
    *)
    echo "unknown option: $key"
    usage
    exit 1
    ;;
esac
done

namespace=${namespace:-pulsar}
release=${release:-pulsar-dev}

function delete_namespace() {
    if [[ "${delete_namespace}" == "true" ]]; then
        kubectl create namespace ${namespace}
    fi
}

# delete the cc admin secrets
kubectl delete -n ${namespace} secret ${release}-admin-secret

# delete tokens
kubectl get secrets -n ${namespace} | grep ${release}-token- | awk '{print $1}' | xargs kubectl delete secrets -n ${namespace}

# delete namespace
delete_namespace
