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

if [ "$#" -lt 1 ]; then
    echo "Need to specify git tag as argument"
    exit 1
fi

export GIT_TAG=$1
export GIT_REPO=${2:-https://github.com/apache/pulsar.git}

echo "GIT_TAG: '$GIT_TAG'"
echo "GIT_REPO: '$GIT_REPO'"

OSX_VERSIONS=(
    osx-10.12
    osx-10.13
    osx-10.14
)

for osx in ${OSX_VERSIONS[@]}; do
    echo ""
    echo "------------- BUILDING PYTHON WHEELS FOR $osx ---------------------"

    pushd $osx
    rm -rf *.whl
    vagrant up --provision
    vagrant scp :/Users/vagrant/pulsar/pulsar-client-cpp/python/dist/*.whl .
    vagrant halt -f
    popd
done
