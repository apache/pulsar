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

set -e
set -x

INSTALL_DIR=/build/third_party
AWS_SDK_CPP_VERSION="1.11.420"
PROTOBUF_VERSION="3.11.4"
BOOST_VERSION="1.76.0"
BOOST_VERSION_UNDERSCORED="${BOOST_VERSION//\./_}"

# Create install directory
mkdir -p $INSTALL_DIR

# Setup environment variables
export CC="gcc"
export CXX="g++"
export CXXFLAGS="-I$INSTALL_DIR/include -O3 -Wno-implicit-fallthrough -Wno-int-in-bool-context"
export LDFLAGS="-L$INSTALL_DIR/lib"
export LD_LIBRARY_PATH="$INSTALL_DIR/lib:$LD_LIBRARY_PATH"

cd $INSTALL_DIR

# Build protobuf
if [ ! -d "protobuf-${PROTOBUF_VERSION}" ]; then
  curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VERSION}/protobuf-all-${PROTOBUF_VERSION}.tar.gz
  tar xf protobuf-all-${PROTOBUF_VERSION}.tar.gz
  rm protobuf-all-${PROTOBUF_VERSION}.tar.gz

  cd protobuf-${PROTOBUF_VERSION}
  ./configure --prefix=${INSTALL_DIR} \
    --disable-shared \
    CFLAGS="-fPIC" \
    CXXFLAGS="-fPIC ${CXXFLAGS}" \
    --with-pic
  make -j4
  make install
  cd ..
fi

# Build Boost
if [ ! -d "boost_${BOOST_VERSION_UNDERSCORED}" ]; then
  curl -LO https://boostorg.jfrog.io/artifactory/main/release/${BOOST_VERSION}/source/boost_${BOOST_VERSION_UNDERSCORED}.tar.gz
  tar xf boost_${BOOST_VERSION_UNDERSCORED}.tar.gz
  rm boost_${BOOST_VERSION_UNDERSCORED}.tar.gz

  cd boost_${BOOST_VERSION_UNDERSCORED}

  BOOST_LIBS="regex,thread,log,system,random,filesystem,chrono,atomic,date_time,program_options,test"

  ./bootstrap.sh --with-libraries=$BOOST_LIBS --with-toolset=gcc

  ./b2 \
    -j4 \
    variant=release \
    link=static \
    threading=multi \
    runtime-link=static \
    --prefix=${INSTALL_DIR} \
    cxxflags="-fPIC ${CXXFLAGS}" \
    install

  cd ..
fi

# Download and build AWS SDK
if [ ! -d "aws-sdk-cpp" ]; then
  git clone --depth 1 --branch ${AWS_SDK_CPP_VERSION} https://github.com/awslabs/aws-sdk-cpp.git aws-sdk-cpp
  pushd aws-sdk-cpp
  git config submodule.fetchJobs 8
  git submodule update --init --depth 1 --recursive
  popd

  rm -rf aws-sdk-cpp-build
  mkdir aws-sdk-cpp-build
  cd aws-sdk-cpp-build

  cmake \
    -DBUILD_ONLY="kinesis;monitoring;sts" \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DSTATIC_LINKING=1 \
    -DCMAKE_PREFIX_PATH="$INSTALL_DIR" \
    -DCMAKE_C_COMPILER="$CC" \
    -DCMAKE_CXX_COMPILER="$CXX" \
    -DCMAKE_CXX_FLAGS="$CXXFLAGS" \
    -DCMAKE_INSTALL_PREFIX="$INSTALL_DIR" \
    -DCMAKE_FIND_FRAMEWORK=LAST \
    -DENABLE_TESTING="OFF" \
    ../aws-sdk-cpp
  make -j4
  make install
  cd ..
fi

# Build the native kinesis producer
cd /build/amazon-kinesis-producer
ln -fs ../third_party
cmake -DCMAKE_PREFIX_PATH="$INSTALL_DIR" -DCMAKE_BUILD_TYPE=RelWithDebInfo .
make -j4

FINAL_DIR=/opt/amazon-kinesis-producer
# copy the binary
mkdir -p $FINAL_DIR/bin
cp kinesis_producer $FINAL_DIR/bin/kinesis_producer.original

# capture version information
git describe --long --tags > $FINAL_DIR/bin/.version
git rev-parse HEAD > $FINAL_DIR/bin/.revision
uname -a > $FINAL_DIR/bin/.system_info
cat /etc/os-release > $FINAL_DIR/bin/.os_info
date > $FINAL_DIR/bin/.build_time

# copy tests
mkdir -p $FINAL_DIR/tests
cp tests $FINAL_DIR/tests/
cp test_driver $FINAL_DIR/tests/

# Strip and compress the binary
cd $FINAL_DIR/bin
strip -o kinesis_producer.stripped kinesis_producer.original
upx --best -o kinesis_producer kinesis_producer.stripped