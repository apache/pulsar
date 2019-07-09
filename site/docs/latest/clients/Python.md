---
title: The Pulsar Python client
tags:
- client
- python
---

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

The Pulsar Python client library is a wrapper over the existing [C++ client library](../Cpp) and exposes all of the [same features](../../../../api/cpp). You can find the code in the [`python` subdirectory]({{ site.pulsar_repo }}/pulsar-client-cpp/python) of the C++ client code.

## Installation

You can install the [`pulsar-client`](https://pypi.python.org/pypi/pulsar-client) library either via [PyPi](https://pypi.python.org/pypi), using [pip](#installation-using-pip), or by building the library from source.

### Installation using pip

To install the `pulsar-client` library as a pre-built package using the [pip](https://pip.pypa.io/en/stable/) package manager:

```shell
$ pip install pulsar-client
```

Installation via PyPi is available for the following Python versions:

Platform | Supported Python versions
:--------|:-------------------------
MacOS 10.12 (Sierra) and 10.13 (High Sierra) | 2.7, 3.6
Linux | 2.7, 3.3, 3.4, 3.5, 3.6

### Installing from source

To install the `pulsar-client` library by building from source, follow [these instructions](../Cpp#compilation) and compile the Pulsar C++ client library. That will also build the Python binding for the library.

To install the built Python bindings:

```shell
$ git clone https://github.com/apache/pulsar
$ cd pulsar/pulsar-client-cpp/python
$ sudo python setup.py install
```

## API Reference

The complete Python API reference is available at [api/python]({{site.baseUrl}}/api/python).

## Examples

Below you'll find a variety of Python code examples for the `pulsar-client` library.

### Producer example

This creates a Python {% popover producer %} for the `my-topic` topic and send 10 messages on that topic:

```python
import pulsar

client = pulsar.Client('pulsar://localhost:6650')

producer = client.create_producer('my-topic')

for i in range(10):
    producer.send(('Hello-%d' % i).encode('utf-8'))

client.close()
```

### Consumer example

This creates a {% popover consumer %} with the `my-subscription` {% popover subscription %} on the `my-topic` topic, listen for incoming messages, print the content and ID of messages that arrive, and {% popover acknowledge %} each message to the Pulsar {% popover broker %}:

```python
consumer = client.subscribe('my-topic', 'my-subscription')

while True:
    msg = consumer.receive()
    print("Received message '{}' id='{}'".format(msg.data(), msg.message_id()))
    consumer.acknowledge(msg)

client.close()
```

### Reader interface example

You can use the Pulsar Python API to use the Pulsar [reader interface](../../getting-started/ConceptsAndArchitecture#reader-interface). Here's an example:

```python
# MessageId taken from a previously fetched message
msg_id = msg.message_id()

reader = client.create_reader('my-topic', msg_id)

while True:
    msg = reader.read_next()
    print("Received message '{}' id='{}'".format(msg.data(), msg.message_id()))
    # No acknowledgment
```
