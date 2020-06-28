---
id: version-2.6.1-develop-cpp
title: Building Pulsar C++ client
sidebar_label: Building Pulsar C++ client
original_id: develop-cpp
---

## Supported platforms

The Pulsar C++ client has been successfully tested on **MacOS** and **Linux**.

## System requirements

You need to have the following installed to use the C++ client:

* [CMake](https://cmake.org/)
* [Boost](http://www.boost.org/)
* [Protocol Buffers](https://developers.google.com/protocol-buffers/) 2.6
* [Log4CXX](https://logging.apache.org/log4cxx)
* [libcurl](https://curl.haxx.se/libcurl/)
* [Google Test](https://github.com/google/googletest)
* [JsonCpp](https://github.com/open-source-parsers/jsoncpp)

## Compilation

There are separate compilation instructions for [MacOS](#macos) and [Linux](#linux). For both systems, start by cloning the Pulsar repository:

```shell
$ git clone https://github.com/apache/pulsar
```

### Linux

First, install all of the necessary dependencies:

```shell
$ apt-get install cmake libssl-dev libcurl4-openssl-dev liblog4cxx-dev \
  libprotobuf-dev protobuf-compiler libboost-all-dev google-mock libgtest-dev libjsoncpp-dev
```

Then compile and install [Google Test](https://github.com/google/googletest):

```shell
# libgtest-dev version is 1.18.0 or above
$ cd /usr/src/googletest
$ sudo cmake .
$ sudo make
$ sudo cp ./googlemock/libgmock.a ./googlemock/gtest/libgtest.a /usr/lib/

# less than 1.18.0
$ cd /usr/src/gtest
$ sudo cmake .
$ sudo make
$ sudo cp libgtest.a /usr/lib

$ cd /usr/src/gmock
$ sudo cmake .
$ sudo make
$ sudo cp libgmock.a /usr/lib
```

Finally, compile the Pulsar client library for C++ inside the Pulsar repo:

```shell
$ cd pulsar-client-cpp
$ cmake .
$ make
```

The resulting files, `libpulsar.so` and `libpulsar.a`, will be placed in the `lib` folder of the repo while two tools, `perfProducer` and `perfConsumer`, will be placed in the `perf` directory.

### MacOS

First, install all of the necessary dependencies:

```shell
# OpenSSL installation
$ brew install openssl
$ export OPENSSL_INCLUDE_DIR=/usr/local/opt/openssl/include/
$ export OPENSSL_ROOT_DIR=/usr/local/opt/openssl/

# Protocol Buffers installation
$ brew tap homebrew/versions
$ brew install protobuf260
$ brew install boost
$ brew install log4cxx

# Google Test installation
$ git clone https://github.com/google/googletest.git
$ cd googletest
$ cmake .
$ make install
```

Then compile the Pulsar client library in the repo that you cloned:

```shell
$ cd pulsar-client-cpp
$ cmake .
$ make
```
