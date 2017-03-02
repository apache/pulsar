#!/bin/bash
#
# Copyright 2016 Yahoo Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
  # Install dependant packages
  exec_cmd "apt-get update && apt-get install -y cmake gcc-4.4 cpp-4.4 gcc-4.4-base libssl-dev libcurl4-openssl-dev liblog4cxx10-dev libprotobuf-dev libboost1.55-all-dev libgtest-dev libxml2-utils g++-4.4";
  exec_cmd "pushd $1/ && wget https://github.com/google/protobuf/releases/download/v2.6.1/protobuf-2.6.1.tar.gz && popd";
  exec_cmd "pushd /usr/src/gtest && CC=gcc-4.4 CXX=g++-4.4 cmake . && make && cp *.a /usr/lib && popd";
  exec_cmd "pushd $1/ && tar xvfz $1/protobuf-2.6.1.tar.gz && pushd $1/protobuf-2.6.1 && ./configure && make && make install && popd && popd";
fi

if [ "$3" = "all" -o "$3" = "compile" ]; then
  # Compile and run unit tests
  exec_cmd "pushd $2/pulsar-client-cpp && CC=gcc-4.4 CXX=g++-4.4 cmake . && make && popd";
  PULSAR_STANDALONE_CONF=$2/pulsar-client-cpp/tests/standalone.conf $2/bin/pulsar standalone &
  standalone_pid=$!;
  PULSAR_STANDALONE_CONF=$2/pulsar-client-cpp/tests/authentication.conf $2/bin/pulsar standalone --zookeeper-port 2191 --bookkeeper-port 3191 --zookeeper-dir data2/standalone/zookeeper --bookkeeper-dir data2/standalone/zookeeper &
  auth_pid=$!;
  sleep 10
  PULSAR_CLIENT_CONF=$2/pulsar-client-cpp/tests/client.conf $2/bin/pulsar-admin clusters create --url http://localhost:9765/ --url-secure https://localhost:9766/ --broker-url pulsar://localhost:9885/ --broker-url-secure pulsar+ssl://localhost:9886/ cluster
  exec_cmd "sleep 5 && pushd $2/pulsar-client-cpp/tests && ./main && popd";
  exec_cmd "kill -SIGTERM $pid";
  exec_cmd "kill -SIGTERM $auth_pid";
fi
