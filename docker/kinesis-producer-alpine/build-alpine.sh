#!/bin/bash

set -e
set -x

INSTALL_DIR=/build/third_party
AWS_SDK_CPP_VERSION="1.11.420"
PROTOBUF_VERSION="3.11.4"

# Create install directory
mkdir -p $INSTALL_DIR

# Setup environment variables
export CC="gcc"
export CXX="g++"
export CXXFLAGS="-I$INSTALL_DIR/include -O3 -Wno-implicit-fallthrough -Wno-int-in-bool-context"
export LDFLAGS="-L$INSTALL_DIR/lib"
export LD_LIBRARY_PATH="$INSTALL_DIR/lib:$LD_LIBRARY_PATH"

cd $INSTALL_DIR

# Build libexecinfo
if [ ! -d "libexecinfo" ]; then
  git clone https://github.com/mikroskeem/libexecinfo
  cd libexecinfo
  make PREFIX=${INSTALL_DIR} install
  cd ..
fi

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
  make -j4 CXXFLAGS="-fPIC ${CXXFLAGS}"
  make install
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
cmake -DCMAKE_PREFIX_PATH="$INSTALL_DIR" -DTHIRD_PARTY_LIB_DIR="$INSTALL_DIR/lib" -DCMAKE_BUILD_TYPE=RelWithDebInfo .
make -j4

# Create directory for the native binary
NATIVE_BINARY_DIR=java/amazon-kinesis-producer/src/main/resources/amazon-kinesis-producer-native-binaries/linux-$(uname -m)/
mkdir -p $NATIVE_BINARY_DIR

# Copy the native producer
cp kinesis_producer $NATIVE_BINARY_DIR

# Build the Java producer
cd java/amazon-kinesis-producer
mvn clean package source:jar javadoc:jar install