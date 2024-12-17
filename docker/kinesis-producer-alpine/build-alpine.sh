#!/bin/bash

set -e
set -x

INSTALL_DIR=/build/third_party
AWS_SDK_CPP_VERSION="1.11.420"

# Create install directory
mkdir -p $INSTALL_DIR

# Setup environment variables
export CC="gcc"
export CXX="g++"
export CXXFLAGS="-I$INSTALL_DIR/include -O3 -Wno-implicit-fallthrough -Wno-int-in-bool-context"
export LDFLAGS="-L$INSTALL_DIR/lib"
export LD_LIBRARY_PATH="$INSTALL_DIR/lib:$LD_LIBRARY_PATH"

# We'll use system libraries for OpenSSL, Boost, zlib, Protobuf, and curl
# Just need to ensure the headers are available

# Download and build AWS SDK
cd $INSTALL_DIR
if [ ! -d "aws-sdk-cpp" ]; then
  git clone --depth 1 --branch ${AWS_SDK_CPP_VERSION} https://github.com/awslabs/aws-sdk-cpp.git aws-sdk-cpp
  pushd aws-sdk-cpp
  git submodule update --init --recursive
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
  make -j$(nproc)
  make install
  cd ..
fi

# Build the native kinesis producer
cd /build/amazon-kinesis-producer
cmake -DCMAKE_PREFIX_PATH="$INSTALL_DIR" -DCMAKE_BUILD_TYPE=RelWithDebInfo .
make -j$(nproc)

# Create directory for the native binary
NATIVE_BINARY_DIR=java/amazon-kinesis-producer/src/main/resources/amazon-kinesis-producer-native-binaries/linux-$(uname -m)/
mkdir -p $NATIVE_BINARY_DIR

# Copy the native producer
cp kinesis_producer $NATIVE_BINARY_DIR

# Build the Java producer
cd java/amazon-kinesis-producer
mvn clean package source:jar javadoc:jar install
