<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Pulsar C++ client library
<!-- TOC depthFrom:2 depthTo:3 withLinks:1 updateOnSave:1 orderedList:0 -->

- [Requirements](#requirements)
- [Platforms](#platforms)
- [Compilation](#compilation)
	- [Compile on Ubuntu Server 16.04](#compile-on-ubuntu-server-1604)
	- [Compile on Mac OS X](#compile-on-mac-os-x)
	- [Compile on Windows (Visual Studio)](#compile-on-windows)
- [Tests](#tests)
- [Requirements for Contributors](#requirements-for-contributors)

<!-- /TOC -->
Examples for using the API to publish and consume messages can be found on
https://github.com/apache/pulsar/tree/master/pulsar-client-cpp/examples

## Requirements

 * A C++ compiler that supports C++11, like GCC >= 4.8
 * CMake >= 3.4
 * [Boost](http://www.boost.org/)
 * [Protocol Buffer](https://developers.google.com/protocol-buffers/) >= 3
 * [libcurl](https://curl.se/libcurl/)
 * [openssl](https://github.com/openssl/openssl)

It's recommended to use Protocol Buffer 2.6 because it's verified by CI, but 3.x also works.

The default supported [compression types](include/pulsar/CompressionType.h) are:

- `CompressionNone`
- `CompressionLZ4`

If you want to enable other compression types, you need to install:

- `CompressionZLib`: [zlib](https://zlib.net/)
- `CompressionZSTD`: [zstd](https://github.com/facebook/zstd)
- `CompressionSNAPPY`: [snappy](https://github.com/google/snappy)

If you want to build and run the tests, you need to install [GTest](https://github.com/google/googletest). Otherwise, you need to add CMake option `-DBUILD_TESTS=OFF`.

If you don't want to build Python client since `boost-python` may not be easy to install, you need to add CMake option `-DBUILD_PYTHON_WRAPPER=OFF`.

If you want to use `ClientConfiguration::setLogConfFilePath`, you need to install the [Log4CXX](https://logging.apache.org/log4cxx) and add CMake option `-DUSE_LOG4CXX=ON`.

## Platforms

Pulsar C++ Client Library has been tested on:

* Linux
* Mac OS X
* Windows x64

## Compilation

### Compile within a Docker container

You can compile the C++ client library within a Docker container that already
contains all the required dependencies.

```shell
./docker-build.sh
```

Run unit tests:
```shell
./docker-tests.sh
```

### Compile on Ubuntu Server 16.04

#### Install all dependencies:

```shell
apt-get install -y g++ cmake libssl-dev libcurl4-openssl-dev liblog4cxx-dev \
                libprotobuf-dev libboost-all-dev  libgtest-dev google-mock \
                protobuf-compiler python-setuptools
```

#### Compile and install Google Test:

```shell
cd /usr/src/gtest
sudo cmake .
sudo make
sudo cp *.a /usr/lib
```


#### Compile and install Google Mock:

```shell
cd /usr/src/gmock
sudo cmake .
sudo make
sudo cp *.a /usr/lib
```


#### Compile Pulsar client library:

```shell
cd pulsar/pulsar-client-cpp
cmake .
make
```

#### Checks
##### Client library will be placed in
```
lib/libpulsar.so
lib/libpulsar.a
```

##### Tools will be placed in

```
perf/perfProducer
perf/perfConsumer
```

### Compile on Mac OS X

#### Install all dependencies:
```shell
# For openSSL
brew install openssl
export OPENSSL_INCLUDE_DIR=/usr/local/opt/openssl/include/
export OPENSSL_ROOT_DIR=/usr/local/opt/openssl/

# For Protobuf
brew install protobuf boost boost-python log4cxx jsoncpp
// If you are using python3, you need to install boost-python3

# For gtest
cd $HOME
git clone https://github.com/google/googletest.git
cd googletest
cmake .
make install
# Refer gtest documentation in case you get stuck somewhere
```

#### Compile Pulsar client library:
```shell
export PULSAR_PATH=<Path where you cloned pulsar repo>
cd ${PULSAR_PATH}/pulsar-client-cpp/
cmake .
make
```

#### Checks
##### Client library will be placed in
```
${PULSAR_PATH}/pulsar-client-cpp/lib/libpulsar.dylib
${PULSAR_PATH}/pulsar-client-cpp/lib/libpulsar.a
```

##### Tools will be placed in:

```
${PULSAR_PATH}/pulsar-client-cpp/perf/perfProducer
${PULSAR_PATH}/pulsar-client-cpp/perf/perfConsumer
```

### Compile on Windows

#### Install with [vcpkg](https://github.com/microsoft/vcpkg)

It's highly recommended to use `vcpkg` for C++ package management on Windows. It's easy to install and well supported by Visual Studio (2015/2017/2019) and CMake. See [here](https://github.com/microsoft/vcpkg#quick-start-windows) for quick start.

Take Windows 64-bit library as an example, you only need to run

```bash
vcpkg install --feature-flags=manifests --triplet x64-windows
```

> NOTE: For Windows 32-bit library, change `x64-windows` to `x86-windows`, see [here](https://github.com/microsoft/vcpkg/blob/master/docs/users/triplets.md) for more details about the triplet concept in Vcpkg.

The all dependencies, which are specified by [vcpkg.json](vcpkg.json), will be installed in `vcpkg_installed/` subdirectory,

With `vcpkg`, you only need to run two commands:

```bash
cmake \
 -B ./build \
 -A x64 \
 -DBUILD_PYTHON_WRAPPER=OFF -DBUILD_TESTS=OFF \
 -DVCPKG_TRIPLET=x64-windows \
 -DCMAKE_BUILD_TYPE=Release \
 -S .
cmake --build ./build --config Release
```

Then all artifacts will be built into `build` subdirectory.

> **NOTE**
>
> 1. For Windows 32-bit, you need to use `-A Win32` and `-DVCPKG_TRIPLET=x86-windows`.
> 2. For MSVC Debug mode, you need to replace `Release` with `Debug` for both `CMAKE_BUILD_TYPE` variable and `--config` option.

#### Install dependencies manually

You need to install [dlfcn-win32](https://github.com/dlfcn-win32/dlfcn-win32) in addition.

If you installed the dependencies manually, you need to run

```shell
#If all dependencies are in your path, all that is necessary is
${PULSAR_PATH}/pulsar-client-cpp/cmake .

#if all dependencies are not in your path, then passing in a PROTOC_PATH and CMAKE_PREFIX_PATH is necessary
${PULSAR_PATH}/pulsar-client-cpp/cmake -DPROTOC_PATH=C:/protobuf/bin/protoc -DCMAKE_PREFIX_PATH="C:/boost;C:/openssl;C:/zlib;C:/curl;C:/protobuf;C:/googletest;C:/dlfcn-win32" .

#This will generate pulsar-cpp.sln. Open this in Visual Studio and build the desired configurations.
```

#### Checks

##### Client libraries are available in the following places.
```
${PULSAR_PATH}/pulsar-client-cpp/build/lib/Release/pulsar.lib
${PULSAR_PATH}/pulsar-client-cpp/build/lib/Release/pulsar.dll
```

#### Examples

##### Add windows environment paths.
```
${PULSAR_PATH}/pulsar-client-cpp/build/lib/Release
${PULSAR_PATH}/pulsar-client-cpp/vcpkg_installed
```

##### Examples are available in.
```
${PULSAR_PATH}/pulsar-client-cpp/build/examples/Release
```

## Tests
```shell
# Source code
${PULSAR_PATH}/pulsar-client-cpp/tests/

# Execution
# Start standalone broker
${PULSAR_PATH}/pulsar-test-service-start.sh

# Run the tests
${PULSAR_PATH}/pulsar-client-cpp/tests/main

# When no longer needed, stop standalone broker
${PULSAR_PATH}/pulsar-test-service-stop.sh
```

## Requirements for Contributors

It's recommended to install [LLVM](https://llvm.org/builds/) for `clang-tidy` and `clang-format`. Pulsar C++ client use `clang-format` 5.0 to format files, which is a little different with latest `clang-format`.

We welcome contributions from the open source community, kindly make sure your changes are backward compatible with GCC 4.8 and Boost 1.53.

### Install `clang-format` on macOS

`homebrew-core` does not have `clang-format@5`. You can install `clang-format@5` on your macOS using the tap below.
```shell
# Step 1: Add tap
brew tap demogorgon314/clang-format

# Step 2: Install clang-format@5
brew install clang-format@5
```
### Install `clang-format` on Ubuntu 18.04
You can find pre-built binaries on the LLVM website: https://releases.llvm.org/download.html#5.0.2

Or you want to use apt install clang-format-5.0.
```shell
sudo apt update
sudo apt install clang-format-5.0
```

