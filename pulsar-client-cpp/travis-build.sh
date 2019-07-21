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

function usage() {
  echo "$0: <unpack location> <build directory> [all|dep|compile]";
  echo "    Unpack Location is used to store downloaded dependant packages";
  echo "    Build directory is pulsar base folder";
  exit 1;
}

if [ $# -ne 3 ]; then
  usage
fi

if [ ! -d $1 ]; then
  echo "Unpack directory $1 does not exists"
  echo ""
  usage;
fi

if [ ! -d $2 ]; then
  echo "Build directory $2 does not exists"
  echo ""
  usage;
fi

if [ "$3" != "all" -a "$3" != "dep" -a "$3" != "compile" ]; then
  echo "Unknown command $3. Supported commands all|dep|compile";
  echo ""
  usage;
fi

exec_cmd() {
  eval $*
  if [ $? -ne 0 ]; then
    echo "Command $* failed"
    usage
  fi
  return $!
}

if [ "$3" = "all" -o "$3" = "dep" ]; then
  sudo find / -name cmake
  sudo add-apt-repository ppa:george-edison55/cmake-3.x -y
  # Install dependant packages
  exec_cmd "apt-get update && apt-get install -y libssl-dev libcurl4-openssl-dev liblog4cxx10-dev protobuf-compiler libprotobuf-dev libboost1.55-all-dev libxml2-utils libjsoncpp-dev";
  exec_cmd "apt-get remove -y cmake cmake-data && apt-get install -y cmake cmake-data"
  exec_cmd "pushd $1/ && git clone https://github.com/google/googletest.git && pushd googletest && cmake . && make && sudo make install && popd && popd";
  if [ ! -d "$1/gtest-parallel/" ]; then
    echo "Not Found: $1/gtest-parallel/"
    exec_cmd "pushd $1/ && git clone https://github.com/google/gtest-parallel.git && popd";
  fi
fi

if [ "$3" = "all" -o "$3" = "compile" ]; then
  export PATH=$PATH:$1/
  # Compile and run unit tests
  pushd $2/pulsar-client-cpp
  cmake -DBUILD_PYTHON_WRAPPER=OFF . && make VERBOSE=1
  if [ $? -ne 0 ]; then
    echo "Failed to compile CPP client library"
    exit 1
  fi
  popd

  PULSAR_STANDALONE_CONF=$2/pulsar-client-cpp/tests/standalone.conf $2/bin/pulsar standalone > broker.log &
  standalone_pid=$!;
  PULSAR_STANDALONE_CONF=$2/pulsar-client-cpp/tests/authentication.conf $2/bin/pulsar standalone \
              --zookeeper-port 2191 --bookkeeper-port 3191 \
              --zookeeper-dir data2/standalone/zookeeper --bookkeeper-dir \
              data2/standalone/zookeeper > broker-tls.log &
  auth_pid=$!;
  sleep 10
  PULSAR_CLIENT_CONF=$2/pulsar-client-cpp/tests/client.conf $2/bin/pulsar-admin clusters create --url http://localhost:4080/ --url-secure https://localhost:8443/ --broker-url pulsar://localhost:6650/ --broker-url-secure pulsar+ssl://localhost:6651/ cluster
  sleep 5
  pushd $2/pulsar-client-cpp/tests
  $1/gtest-parallel/gtest-parallel ./main --workers=10
  RES=$?
  popd
  exec_cmd "kill -SIGTERM $standalone_pid";
  exec_cmd "kill -SIGTERM $auth_pid";

  if [ $RES -ne 0 ]; then
    echo "Unit tests failed"
    exit 1
  fi
fi
