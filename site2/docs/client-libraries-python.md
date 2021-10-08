---
id: client-libraries-python
title: Pulsar Python client
sidebar_label: Python
---

Pulsar Python client library is a wrapper over the existing [C++ client library](client-libraries-cpp.md) and exposes all of the [same features](/api/cpp). You can find the code in the [Python directory](https://github.com/apache/pulsar/tree/master/pulsar-client-cpp/python) of the C++ client code.

All the methods in producer, consumer, and reader of a Python client are thread-safe.

[pdoc](https://github.com/BurntSushi/pdoc)-generated API docs for the Python client are available [here](/api/python).

## Install

You can install the [`pulsar-client`](https://pypi.python.org/pypi/pulsar-client) library either via [PyPi](https://pypi.python.org/pypi), using [pip](#installation-using-pip), or by building the library from [source](https://github.com/apache/pulsar/tree/master/pulsar-client-cpp).

### Install using pip

To install the `pulsar-client` library as a pre-built package using the [pip](https://pip.pypa.io/en/stable/) package manager:

```shell
$ pip install pulsar-client=={{pulsar:version_number}}
```

### Optional dependencies
If you install the client libraries on Linux to support services like Pulsar functions or Avro serialization, you can install optional components alongside the  `pulsar-client` library.

```shell
# avro serialization
$ pip install pulsar-client=='{{pulsar:version_number}}[avro]'

# functions runtime
$ pip install pulsar-client=='{{pulsar:version_number}}[functions]'

# all optional components
$ pip install pulsar-client=='{{pulsar:version_number}}[all]'
```

Installation via PyPi is available for the following Python versions:

Platform | Supported Python versions
:--------|:-------------------------
MacOS <br />  10.13 (High Sierra), 10.14 (Mojave) <br /> | 2.7, 3.7
Linux | 2.7, 3.4, 3.5, 3.6, 3.7, 3.8

### Install from source

To install the `pulsar-client` library by building from source, follow [instructions](client-libraries-cpp.md#compilation) and compile the Pulsar C++ client library. That builds the Python binding for the library.

To install the built Python bindings:

```shell
$ git clone https://github.com/apache/pulsar
$ cd pulsar/pulsar-client-cpp/python
$ sudo python setup.py install
```

## API Reference

The complete Python API reference is available at [api/python](/api/python).

## Examples

You can find a variety of Python code examples for the [pulsar-client](/pulsar-client-cpp/python) library.

### Producer example

The following example creates a Python producer for the `my-topic` topic and sends 10 messages on that topic:

```python
import pulsar

client = pulsar.Client('pulsar://localhost:6650')

producer = client.create_producer('my-topic')

for i in range(10):
    producer.send(('Hello-%d' % i).encode('utf-8'))

client.close()
```

### Consumer example

The following example creates a consumer with the `my-subscription` subscription name on the `my-topic` topic, receives incoming messages, prints the content and ID of messages that arrive, and acknowledges each message to the Pulsar broker.

```python
import pulsar

client = pulsar.Client('pulsar://localhost:6650')

consumer = client.subscribe('my-topic', 'my-subscription')

while True:
    msg = consumer.receive()
    try:
        print("Received message '{}' id='{}'".format(msg.data(), msg.message_id()))
        # Acknowledge successful processing of the message
        consumer.acknowledge(msg)
    except:
        # Message failed to be processed
        consumer.negative_acknowledge(msg)

client.close()
```

This example shows how to configure negative acknowledgement.

```python
from pulsar import Client, schema
client = Client('pulsar://localhost:6650')
consumer = client.subscribe('negative_acks','test',schema=schema.StringSchema())
producer = client.create_producer('negative_acks',schema=schema.StringSchema())
for i in range(10):
    print('send msg "hello-%d"' % i)
    producer.send_async('hello-%d' % i, callback=None)
producer.flush()
for i in range(10):
    msg = consumer.receive()
    consumer.negative_acknowledge(msg)
    print('receive and nack msg "%s"' % msg.data())
for i in range(10):
    msg = consumer.receive()
    consumer.acknowledge(msg)
    print('receive and ack msg "%s"' % msg.data())
try:
    # No more messages expected
    msg = consumer.receive(100)
except:
    print("no more msg")
    pass
```

### Reader interface example

You can use the Pulsar Python API to use the Pulsar [reader interface](concepts-clients.md#reader-interface). Here's an example:

```python
# MessageId taken from a previously fetched message
msg_id = msg.message_id()

reader = client.create_reader('my-topic', msg_id)

while True:
    msg = reader.read_next()
    print("Received message '{}' id='{}'".format(msg.data(), msg.message_id()))
    # No acknowledgment
```
### Multi-topic subscriptions

In addition to subscribing a consumer to a single Pulsar topic, you can also subscribe to multiple topics simultaneously. To use multi-topic subscriptions, you can supply a regular expression (regex) or a `List` of topics. If you select topics via regex, all topics must be within the same Pulsar namespace.

The following is an example: 

```python
import re
consumer = client.subscribe(re.compile('persistent://public/default/topic-*'), 'my-subscription')
while True:
    msg = consumer.receive()
    try:
        print("Received message '{}' id='{}'".format(msg.data(), msg.message_id()))
        # Acknowledge successful processing of the message
        consumer.acknowledge(msg)
    except:
        # Message failed to be processed
        consumer.negative_acknowledge(msg)
client.close()
```

## Schema

### Declare and validate schema

You can declare a schema by passing a class that inherits
from `pulsar.schema.Record` and defines the fields as
class variables. For example:

```python
from pulsar.schema import *

class Example(Record):
    a = String()
    b = Integer()
    c = Boolean()
```

With this simple schema definition, you can create producers, consumers and readers instances that refer to that.

```python
producer = client.create_producer(
                    topic='my-topic',
                    schema=AvroSchema(Example) )

producer.send(Example(a='Hello', b=1))
```

After creating the producer, the Pulsar broker validates that the existing topic schema is indeed of "Avro" type and that the format is compatible with the schema definition of the `Example` class.

If there is a mismatch, an exception occurs in the producer creation.

Once a producer is created with a certain schema definition,
it will only accept objects that are instances of the declared
schema class.

Similarly, for a consumer/reader, the consumer will return an
object, instance of the schema record class, rather than the raw
bytes:

```python
consumer = client.subscribe(
                  topic='my-topic',
                  subscription_name='my-subscription',
                  schema=AvroSchema(Example) )

while True:
    msg = consumer.receive()
    ex = msg.value()
    try:
        print("Received message a={} b={} c={}".format(ex.a, ex.b, ex.c))
        # Acknowledge successful processing of the message
        consumer.acknowledge(msg)
    except:
        # Message failed to be processed
        consumer.negative_acknowledge(msg)
```

### Supported schema types

You can use different builtin schema types in Pulsar. All the definitions are in the `pulsar.schema` package.

| Schema | Notes |
| ------ | ----- |
| `BytesSchema` | Get the raw payload as a `bytes` object. No serialization/deserialization are performed. This is the default schema mode |
| `StringSchema` | Encode/decode payload as a UTF-8 string. Uses `str` objects |
| `JsonSchema` | Require record definition. Serializes the record into standard JSON payload |
| `AvroSchema` | Require record definition. Serializes in AVRO format |

### Schema definition reference

The schema definition is done through a class that inherits from `pulsar.schema.Record`.

This class has a number of fields which can be of either
`pulsar.schema.Field` type or another nested `Record`. All the
fields are specified in the `pulsar.schema` package. The fields
are matching the AVRO fields types.

| Field Type | Python Type | Notes |
| ---------- | ----------- | ----- |
| `Boolean`  | `bool`      |       |
| `Integer`  | `int`       |       |
| `Long`     | `int`       |       |
| `Float`    | `float`     |       |
| `Double`   | `float`     |       |
| `Bytes`    | `bytes`     |       |
| `String`   | `str`       |       |
| `Array`    | `list`      | Need to specify record type for items. |
| `Map`      | `dict`      | Key is always `String`. Need to specify value type. |

Additionally, any Python `Enum` type can be used as a valid field type.

#### Fields parameters

When adding a field, you can use these parameters in the constructor.

| Argument   | Default | Notes |
| ---------- | --------| ----- |
| `default`  | `None`  | Set a default value for the field. Eg: `a = Integer(default=5)` |
| `required` | `False` | Mark the field as "required". It is set in the schema accordingly. |

#### Schema definition examples

##### Simple definition

```python
class Example(Record):
    a = String()
    b = Integer()
    c = Array(String())
    i = Map(String())
```

##### Using enums

```python
from enum import Enum

class Color(Enum):
    red = 1
    green = 2
    blue = 3

class Example(Record):
    name = String()
    color = Color
```

##### Complex types

```python
class MySubRecord(Record):
    x = Integer()
    y = Long()
    z = String()

class Example(Record):
    a = String()
    sub = MySubRecord()
```

##### Set namespace for Avro schema

Set the namespace for Avro Record schema using the special field `_avro_namespace`.
```python
class NamespaceDemo(Record):
   _avro_namespace = 'xxx.xxx.xxx'
   x = String()
   y = Integer()
```

The schema definition is like this.
```
{
  'name': 'NamespaceDemo', 'namespace': 'xxx.xxx.xxx', 'type': 'record', 'fields': [
    {'name': 'x', 'type': ['null', 'string']}, 
    {'name': 'y', 'type': ['null', 'int']}
  ]
}
```

## End-to-end encryption

[End-to-end encryption](https://pulsar.apache.org/docs/en/next/cookbooks-encryption/#docsNav) allows applications to encrypt messages at producers and decrypt messages at consumers.

### Configuration

To use the end-to-end encryption feature in the Python client, you need to configure `publicKeyPath` and `privateKeyPath` for both producer and consumer.

```
publicKeyPath: "./public.pem"
privateKeyPath: "./private.pem"
```

### Tutorial

This section provides step-by-step instructions on how to use the end-to-end encryption feature in the Python client.

**Prerequisite**

- Pulsar Python client 2.7.1 or later 

**Step**

1. Create both public and private key pairs.

    **Input**

    ```shell
    openssl genrsa -out private.pem 2048
    openssl rsa -in private.pem -pubout -out public.pem
    ```

2. Create a producer to send encrypted messages.

    **Input**

    ```python
    import pulsar

    publicKeyPath = "./public.pem"
    privateKeyPath = "./private.pem"
    crypto_key_reader = pulsar.CryptoKeyReader(publicKeyPath, privateKeyPath)
    client = pulsar.Client('pulsar://localhost:6650')
    producer = client.create_producer(topic='encryption', encryption_key='encryption', crypto_key_reader=crypto_key_reader)
    producer.send('encryption message'.encode('utf8'))
    print('sent message')
    producer.close()
    client.close()
    ```

3. Create a consumer to receive encrypted messages.

    **Input**

    ```python
    import pulsar

    publicKeyPath = "./public.pem"
    privateKeyPath = "./private.pem"
    crypto_key_reader = pulsar.CryptoKeyReader(publicKeyPath, privateKeyPath)
    client = pulsar.Client('pulsar://localhost:6650')
    consumer = client.subscribe(topic='encryption', subscription_name='encryption-sub', crypto_key_reader=crypto_key_reader)
    msg = consumer.receive()
    print("Received msg '{}' id = '{}'".format(msg.data(), msg.message_id()))
    consumer.close()
    client.close()
    ```

4. Run the consumer to receive encrypted messages.

    **Input**

    ```shell
    python consumer.py
    ```

5. In a new terminal tab, run the producer to produce encrypted messages.

    **Input**

    ```shell
    python producer.py
    ```

    Now you can see the producer sends messages and the consumer receives messages successfully.

    **Output**

    This is from the producer side.

    ```
    sent message
    ```

    This is from the consumer side.

    ```
    Received msg 'encryption message' id = '(0,0,-1,-1)'
    ```
