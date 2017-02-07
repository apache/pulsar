
### Pulsar C++ client library

Examples for using the API to publish and consume messages can be found on
https://github.com/yahoo/pulsar/tree/master/pulsar-client-cpp/examples

#### Requirements

 * CMake
 * Boost
 * [Protocol Buffer 2.6](https://developers.google.com/protocol-buffers/)
 * [Log4CXX](https://logging.apache.org/log4cxx)
 * LibCurl
 * [GTest](https://github.com/google/googletest)


#### Compile on Ubuntu Server 16.04

Install all dependencies:

```shell
apt-get install cmake libssl-dev libcurl4-openssl-dev liblog4cxx-dev \
                libprotobuf-dev libboost-all-dev  libgtest-dev
```

Compile and install Google Test:

```shell
cd /usr/src/gtest
sudo cmake .
sudo make
sudo cp *.a /usr/lib
```


Compile Pulsar client library:

```shell
cd pulsar/pulsar-client-cpp
cmake .
make
```

Client library will be placed in
```
lib/libpulsar.so
lib/libpulsar.a
```

Tools :

```
perf/perfProducer
perf/perfConsumer
```

Tests:

```
 1. Start the standalone pulsar
    export PULSAR_STANDALONE_CONF=tests/standalone.conf
    ../bin/pulsar standalone

 2. Run tests
    tests/main
```
