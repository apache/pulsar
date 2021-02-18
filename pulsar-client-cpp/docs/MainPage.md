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

# The Pulsar C++ client

Welcome to the Doxygen documentation for [Pulsar](https://pulsar.apache.org/).

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

## Compilation

There are separate compilation instructions for [MacOS](#macos) and [Linux](#linux). For both systems, start by cloning the Pulsar repository:

```shell
$ git clone https://github.com/apache/pulsar
```

### Linux

First, install all of the necessary dependencies:

```shell
$ apt-get install cmake libssl-dev libcurl4-openssl-dev liblog4cxx-dev \
  libprotobuf-dev libboost-all-dev libgtest-dev libjsoncpp-dev
```

Then compile and install [Google Test](https://github.com/google/googletest):

```shell
$ git clone https://github.com/google/googletest.git && cd googletest
$ sudo cmake .
$ sudo make
$ sudo cp *.a /usr/lib
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

## Consumer

```cpp
Client client("pulsar://localhost:6650");

Consumer consumer;
Result result = client.subscribe("persistent://sample/standalone/ns1/my-topic", "my-subscribtion-name", consumer);
if (result != ResultOk) {
    LOG_ERROR("Failed to subscribe: " << result);
    return -1;
}

Message msg;

while (true) {
    consumer.receive(msg);
    LOG_INFO("Received: " << msg << "  with payload '" << msg.getDataAsString() << "'");

    consumer.acknowledge(msg);
}

client.close();
```


## Producer

```cpp
Client client("pulsar://localhost:6650");

Producer producer;
Result result = client.createProducer("persistent://sample/standalone/ns1/my-topic", producer);
if (result != ResultOk) {
    LOG_ERROR("Error creating producer: " << result);
    return -1;
}

// Publish 10 messages to the topic
for(int i=0;i<10;i++){
    Message msg = MessageBuilder().setContent("my-message").build();
    Result res = producer.send(msg);
    LOG_INFO("Message sent: " << res);
}
client.close();
```

## Authentication

```cpp
ClientConfiguration config = ClientConfiguration();
config.setUseTls(true);
std::string certfile = "/path/to/cacert.pem";

ParamMap params;
params["tlsCertFile"] = "/path/to/client-cert.pem";
params["tlsKeyFile"]  = "/path/to/client-key.pem";
config.setTlsTrustCertsFilePath(certfile);
config.setTlsAllowInsecureConnection(false);
AuthenticationPtr auth = pulsar::AuthFactory::create("/path/to/libauthtls.so", params);
config.setAuth(auth);

Client client("pulsar+ssl://my-broker.com:6651",config);
```

## Code formatting
After you changed code, run auto-formatting by the following command.

```bash
make format
```
You need to have the following installed to use the auto-formatting.
* [clang-format 5.0](https://clang.llvm.org/)
