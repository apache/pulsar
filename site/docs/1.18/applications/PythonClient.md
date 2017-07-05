---
title: The Pulsar Python client
tags:
- client
- python
---

The Pulsar Python client library is a wrapper over the existing [C++ client library](../CppClient) and exposes all of the [same features](../../../../api/cpp). You can find the code in the [`python` subdirectory]({{ site.pulsar_repo }}/pulsar-client-cpp/python) of the C++ client code.


## Installation

You can install the `pulsar-client` library either using [pip](https://pip.pypa.io/en/stable/) or by building the library from source.

To install using pip:

```shell
$ pip install pulsar-client
```

To install by building from source, follow the [instructions](../CppClient#compilation) and compile the Pulsar C++ client library. That will also build the Python binding for the library.

To install the built Python bindings:

```shell
$ git clone https://github.com/apache/pulsar
$ cd pulsar/pulsar-client-cpp/python
$ sudo python setup.py install
```

{% include admonition.html type="info" content="Currently, the only supported Python version is 2.7." %}

## Examples

Below you'll find a variety of Python code examples for the `pulsar-client` library.

### Producer example

This would create a Python {% popover producer %} for the `persistent://sample/standalone/ns/my-topic` topic and send 10 messages on that topic:

```python
import pulsar

client = pulsar.Client('pulsar://localhost:6650')

producer = client.create_producer(
                'persistent://sample/standalone/ns/my-topic')

for i in range(10):
    producer.send('Hello-%d' % i)

client.close()
```

### Consumer example

This would create a {% popover consumer %} with the `my-sub` {% popover subscription %} on the `persistent://sample/standalone/ns/my-topic` topic, listen for incoming messages, print the content and ID of messages that arrive, and {% popover acknowledge %} each message to the Pulsar {% popover broker %}:

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

### Async producer example

This would create a Pulsar {% popover producer %} that sends messages asynchronously and triggers the `send_callback` callback function whenever messages are {% popover acknowledged %} by the {% popover broker %}:

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
