---
title: The Pulsar Python client
tags:
- client
- python
---

The Pulsar Python client library is a wrapper over the existing [C++ client library](../CppClient) and exposes all of the [same features]({{ site.baseurl }}api/cpp). You can find the code in the [`python` subdirectory]({{ site.pulsar_repo }}/pulsar-client-cpp/python) of the C++ client code.


## Installation

First, follow the [instructions](../CppClient#compilation) to compile the Pulsar C++ client library. That will also build the Python binding for the library.

Currently, the only supported Python version is 2.7.

To install the Python bindings:

```shell
$ cd pulsar-client-cpp/python
$ sudo python setup.py install
```

## Examples

### Producer example

```python
import pulsar

client = pulsar.Client('pulsar://localhost:6650')

producer = client.create_producer(
                'persistent://sample/standalone/ns/my-topic')

for i in range(10):
    producer.send('Hello-%d' % i)

client.close()
```

### Consumer Example

```python
import pulsar

client = pulsar.Client('pulsar://localhost:6650')
consumer = client.subscribe(
        'persistent://sample/standalone/ns/my-topic',
        'my-sub')

while True:
    msg = consumer.receive()
    print("Received message '%s' id='%s'", msg.data(), msg.message_id())
    consumer.acknowledge(msg)

client.close()
```

### Async Producer example

```python
import pulsar

client = pulsar.Client('pulsar://localhost:6650')

producer = client.create_producer(
                'persistent://sample/standalone/ns/my-topic',
                block_if_queue_full=True,
                batching_enabled=True,
                batching_max_publish_delay_ms=10
            )

def send_callback(res, msg):
    print('Message published res=%s', res)

while True:
    producer.send_async('Hello-%d' % i, send_callback)

client.close()
```
