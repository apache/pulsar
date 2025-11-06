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
SILENT="n"

silence() {
  if [ -n "$SILENT" ]; then
    "$@" >/dev/null
  else
    "$@"
  fi
}

BOOST_VERSION="1.88.0"
BOOST_VERSION_UNDERSCORED="${BOOST_VERSION//\./_}" # convert from 1.76.0 to 1_76_0
PROTOBUF_VERSION="21.12"
AWS_SDK_CPP_VERSION="1.11.615"

LIB_BOOST="https://archives.boost.io/release/${BOOST_VERSION}/source/boost_${BOOST_VERSION_UNDERSCORED}.tar.gz"
LIB_PROTOBUF="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VERSION}/protobuf-all-${PROTOBUF_VERSION}.tar.gz"

INSTALL_DIR=/build/third_party
# Create install directory
mkdir -p $INSTALL_DIR

# Setup environment variables
export CC="gcc"
export CXX="g++"
export CXXFLAGS="-I$INSTALL_DIR/include -O3 -Wno-implicit-fallthrough -Wno-int-in-bool-context"
export LDFLAGS="-L$INSTALL_DIR/lib"
export LD_LIBRARY_PATH="$INSTALL_DIR/lib:$LD_LIBRARY_PATH"

cd $INSTALL_DIR

SED="sed -i"
CMAKE=$(which cmake3 &>/dev/null && echo "cmake3 " || echo "cmake")
function _curl {
  curl -L "$@"
}

function conf {
  silence ./configure \
    --prefix="$INSTALL_DIR" \
    LD_LIBRARY_PATH="$LD_LIBRARY_PATH" \
    LDFLAGS="$LDFLAGS" \
    CXXFLAGS="$CXXFLAGS" \
    C_INCLUDE_PATH="$C_INCLUDE_PATH" \
    "$@"
}

# Boost C++ Libraries
if [ ! -d "boost_${BOOST_VERSION_UNDERSCORED}" ]; then
  _curl "$LIB_BOOST" >boost.tgz
  tar xf boost.tgz
  rm boost.tgz

  cd boost_${BOOST_VERSION_UNDERSCORED}

  LIBS="atomic,chrono,log,system,test,random,regex,thread,filesystem"
  OPTS="--build-type=minimal --layout=system --prefix=$INSTALL_DIR link=static threading=multi release install"

  silence ./bootstrap.sh --with-libraries="$LIBS" --with-toolset=gcc
  silence ./b2 toolset=gcc $OPTS

  cd ..
fi

# Google Protocol Buffers
if [ ! -d "protobuf-${PROTOBUF_VERSION}" ]; then
  _curl "$LIB_PROTOBUF" >protobuf.tgz
  tar xf protobuf.tgz
  rm protobuf.tgz

  cd protobuf-${PROTOBUF_VERSION}
  silence conf --enable-shared=no
  silence make -j 4
  silence make install

  cd ..
fi

# AWS C++ SDK
if [ ! -d "aws-sdk-cpp" ]; then
  git clone https://github.com/awslabs/aws-sdk-cpp.git aws-sdk-cpp
  pushd aws-sdk-cpp
  git checkout ${AWS_SDK_CPP_VERSION}
  git submodule update --init --recursive
  popd

  rm -rf aws-sdk-cpp-build
  mkdir aws-sdk-cpp-build

  cd aws-sdk-cpp-build

  silence $CMAKE \
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
  silence make -j4
  silence make install

  cd ..

fi

cd ..

# Build the native kinesis producer
cd /build/amazon-kinesis-producer
ln -fs ../third_party
$CMAKE -DCMAKE_PREFIX_PATH="$INSTALL_DIR" -DCMAKE_BUILD_TYPE=RelWithDebInfo .
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