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

set -e
set -o pipefail

function set_pulsar_mem() {
  local maxMem=$1
  local additionalMemParam=$2
  local pulsar_test_mem
  # set into pulsar_test_mem while trimming whitespace
  read -r pulsar_test_mem <<< "-Xmx${maxMem} ${additionalMemParam}"
  # prefer PULSAR_MEM, but always append params to perform a heap dump on OOME
  export PULSAR_MEM="${PULSAR_MEM:-"${pulsar_test_mem}"} -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/var/log/pulsar -XX:+ExitOnOutOfMemoryError"
}

function run_pulsar_component() {
  local component=$1
  local supervisord_component=$2
  local maxMem=$3
  local additionalMemParam=$4

  set_pulsar_mem "$maxMem" "$additionalMemParam"

  if [[ -f "conf/${component}.conf" ]]; then
    bin/apply-config-from-env.py conf/${component}.conf
  fi
  bin/apply-config-from-env.py conf/client.conf

  if [[ "$component" == "functions_worker" ]]; then
    bin/gen-yml-from-env.py conf/functions_worker.yml
  fi

  if [[ "${supervisord_component}" == "global-zk" ]]; then
    bin/generate-zookeeper-config.sh conf/global_zookeeper.conf
  elif [[ "${supervisord_component}" == "local-zk" ]]; then
    bin/generate-zookeeper-config.sh conf/zookeeper.conf
  fi

  if [ -z "$NO_AUTOSTART" ]; then
    sed -i 's/autostart=.*/autostart=true/' /etc/supervisord/conf.d/${supervisord_component}.conf
  fi

  exec /usr/bin/supervisord -c /etc/supervisord.conf
}