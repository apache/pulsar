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
- [Tests](#tests)
- [Requirements for Contributors](#requirements-for-contributors)

<!-- /TOC -->
Examples for using the API to publish and consume messages can be found on
https://github.com/apache/pulsar/tree/master/pulsar-client-cpp/examples

## Requirements

 * CMake
 * [Boost](http://www.boost.org/)
 * [Protocol Buffer 2.6](https://developers.google.com/protocol-buffers/)
 * [Log4CXX](https://logging.apache.org/log4cxx)
 * LibCurl
 * [GTest](https://github.com/google/googletest)
 * JsonCpp


## Platforms

Pulsar C++ Client Library has been tested on:

* Linux
* Mac OS X

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
                libjsoncpp-dev libxml2-utils protobuf-compiler python-setuptools
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
brew tap homebrew/versions
brew install protobuf260 boost boost-python log4cxx jsoncpp

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

## Tests
```shell
# Source code
${PULSAR_PATH}/pulsar-client-cpp/tests/

# Execution
# Start standalone broker
export PULSAR_STANDALONE_CONF=${PULSAR_PATH}/pulsar-client-cpp/tests/standalone.conf
${PULSAR_PATH}/bin/pulsar standalone

# Run the tests
${PULSAR_PATH}/pulsar-client-cpp/tests/main
```

## Requirements for Contributors
We welcome contributions from the open source community, kindly make sure your changes are backward compatible with gcc-4.4.7 and Boost 1.41.
