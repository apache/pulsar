
# Pulsar C++ client library

Examples for using the API to publish and consume messages can be found on
https://github.com/yahoo/pulsar/tree/master/pulsar-client-cpp/examples

## Requirements

 * CMake
 * [Boost](http://www.boost.org/)
 * [Protocol Buffer 2.6](https://developers.google.com/protocol-buffers/)
 * [Log4CXX](https://logging.apache.org/log4cxx)
 * LibCurl
 * [GTest](https://github.com/google/googletest)

## Platforms

Pulsar C++ Client Library has been tested on:

* Linux
* Mac OS X

## Compilation
### Compile on Ubuntu Server 16.04

#### Install all dependencies:

```shell
apt-get install cmake libssl-dev libcurl4-openssl-dev liblog4cxx-dev \
                libprotobuf-dev libboost-all-dev  libgtest-dev
```

#### Compile and install Google Test:

```shell
cd /usr/src/gtest
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

# For Protobuf
brew tap homebrew/versions
brew install protobuf260

# For gtest
cd $HOME
git clone https://github.com/google/googletest.git
cd googletest
cmake .
make 
## Refer gtest documentation in case you get stuck somewhere
export PATH=$HOME/googletest/build/googlemock/gtest/:$HOME/googletest/googletest/include/:$PATH
```

#### Compile Pulsar client library:
```shell
export $PULSAR_PATH=<Path where you cloned pulsar repo>
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
### Tests are placed in 
```
# Source code
${PULSAR_PATH}/pulsar-client-cpp/tests/

# Execution
## Start standalone broker
export PULSAR_STANDALONE_CONF=${PULSAR_PATH}/pulsar-client-cpp/tests/standalone.conf
${PULSAR_PATH}/bin/pulsar standalone

## Run the tests
${PULSAR_PATH}/pulsar-client-cpp/tests/main
```

## Requirements for Contributors
We welcome contributions from the open source community, kindly make sure your changes are backward compatible with gcc-4.4.7 and Boost 1.41.
