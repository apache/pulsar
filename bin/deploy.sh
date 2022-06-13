#!/bin/bash
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

echoerr() { echo "$@" 1>&2; }

cluster_name=$1
module=$2 # puller/shipper/clean
component=$3 # eslog/hdfs/clean/druid
channel_name=$4
cluster_rest_domian=$5
rest_port=$6
datahubapi=$7

workdir=$( cd $(dirname $0) && pwd )

echo "Starting to deploy ${cluster_name}"

register_cluster() {
    echo curl -s http://$datahubapi/v3/databus/clusters/$cluster_name/
    content=$(curl -s http://$datahubapi/v3/databus/clusters/$cluster_name/)
    if [[ $? != 0 ]]; then
        echo "register error $content"
        exit 1
    fi
    echo $content | sed -e 's/ //g' | grep -q '"result":true' && return

    echo curl -s -XPOST -H "Content-Type: application/json" -d '{"component":"'${component}'","module":"'${module}'","channel_name":"'${channel_name}'","cluster_name": "'${cluster_name}'", "cluster_rest_port": '${rest_port}', "cluster_rest_domain": "'${cluster_rest_domian}'", "tags": ["inland"], "cluster_type": "pulsar"}' http://$datahubapi/v3/databus/clusters/
    content=$(curl -s -XPOST -H "Content-Type: application/json" -d '{"component":"'${component}'","module":"'${module}'","channel_name":"'${channel_name}'","cluster_name": "'${cluster_name}'", "cluster_rest_port": '${rest_port}', "cluster_rest_domain": "'${cluster_rest_domian}'", "tags": ["inland"], "cluster_type": "pulsar"}' http://$datahubapi/v3/databus/clusters/)
    if [[ $? != 0 ]]; then
        echo "register error $content"
        exit 1
    fi
    echo $content | sed -e 's/ //g' | grep -q '"result":false' && exit 1
}

register_cluster

echo "done"
