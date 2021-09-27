---
id: version-2.3.1-client-libraries-python
title: The Pulsar Python client
sidebar_label: Python
original_id: client-libraries-python
---

The Pulsar Python client library is a wrapper over the existing [C++ client library](client-libraries-cpp.md) and exposes all of the [same features](/api/cpp). You can find the code in the [`python` subdirectory](https://github.com/apache/pulsar/tree/master/pulsar-client-cpp/python) of the C++ client code.

## Installation

You can install the [`pulsar-client`](https://pypi.python.org/pypi/pulsar-client) library either via [PyPi](https://pypi.python.org/pypi), using [pip](#installation-using-pip), or by building the library from source.

### Installation using pip

To install the `pulsar-client` library as a pre-built package using the [pip](https://pip.pypa.io/en/stable/) package manager:

```shell
$ pip install pulsar-client=={{pulsar:version_number}}
```

Installation via PyPi is available for the following Python versions:

Platform | Supported Python versions
:--------|:-------------------------
MacOS <br /> 10.11 (El Capitan) &mdash; 10.12 (Sierra) &mdash; <br /> 10.13 (High Sierra) &mdash; 10.14 (Mojave)  | 2.7, 3.7
Linux | 2.7, 3.4, 3.5, 3.6, 3.7

### Installing from source

To install the `pulsar-client` library by building from source, follow [these instructions](client-libraries-cpp.md#compilation) and compile the Pulsar C++ client library. That will also build the Python binding for the library.

To install the built Python bindings:

```shell
$ git clone https://github.com/apache/pulsar
$ cd pulsar/pulsar-client-cpp/python
$ sudo python setup.py install
```

## API Reference

The complete Python API reference is available at [api/python](/api/python).

## Examples

Below you'll find a variety of Python code examples for the `pulsar-client` library.

### Producer example

This creates a Python producer for the `my-topic` topic and send 10 messages on that topic:

```python
import pulsar

client = pulsar.Client('pulsar://localhost:6650')

producer = client.create_producer('my-topic')

for i in range(10):
    producer.send(('Hello-%d' % i).encode('utf-8'))

client.close()
```

### Consumer example

This creates a consumer with the `my-subscription` subscription on the `my-topic` topic, listen for incoming messages, print the content and ID of messages that arrive, and acknowledge each message to the Pulsar broker:

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


## Schema

### Declaring and validating schema

A schema can be declared by passing a class that inherits
from `pulsar.schema.Record` and defines the fields as
class variables. For example:

```python
from pulsar.schema import *

class Example(Record):
    a = String()
    b = Integer()
    c = Boolean()
```

With this simple schema definition we can then create producers,
consumers and readers instances that will be referring to that.

```python
producer = client.create_producer(
                    topic='my-topic',
                    schema=AvroSchema(Example) )

producer.send(Example(a='Hello', b=1))
```

When the producer is created, the Pulsar broker will validate that
the existing topic schema is indeed of "Avro" type and that the
format is compatible with the schema definition of the `Example`
class.

If there is a mismatch, the producer creation will raise an
exception.

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

There are different builtin schema types that can be used in Pulsar.
All the definitions are in the `pulsar.schema` package.

| Schema | Notes |
| ------ | ----- |
| `BytesSchema` | Get the raw payload as a `bytes` object. No serialization/deserialization are performed. This is the default schema mode |
| `StringSchema` | Encode/decode payload as a UTF-8 string. Uses `str` objects |
| `JsonSchema` | Require record definition. Serializes the record into standard JSON payload |
| `AvroSchema` | Require record definition. Serializes in AVRO format |

### Schema definition reference

The schema definition is done through a class that inherits from
`pulsar.schema.Record`.

This class can have a number of fields which can be of either
`pulsar.schema.Field` type or even another nested `Record`. All the
fields are also specified in the `pulsar.schema` package. The fields
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
| `Array`    | `list`      | Need to specify record type for items |
| `Map`      | `dict`      | Key is always `String`. Need to specify value type |

Additionally, any Python `Enum` type can be used as a valid field
type

#### Fields parameters

When adding a field these parameters can be used in the constructor:

| Argument   | Default | Notes |
| ---------- | --------| ----- |
| `default`  | `None`  | Set a default value for the field. Eg: `a = Integer(default=5)` |
| `required` | `False` | Mark the field as "required". This will set it in the schema accordingly. |

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
