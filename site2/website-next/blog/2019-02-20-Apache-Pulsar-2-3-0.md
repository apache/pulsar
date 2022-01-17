---
author: Matteo Merli
authorURL: https://twitter.com/merlimat
title: Apache Pulsar 2.3.0
---

The Apache Pulsar PMC is happy to announce the release of Pulsar 2.3.0. This
is the result of huge effort from the community, with over 480 commits and
a long list of new features, general improvements and bug fixes.

These improvements have been across the board in all of Pulsar components,
from new messaging features, to improved usability for Pulsar Functions
and Pulsar IO.

Check out the official <b>[release notes](/release-notes/#2.3.0)</b> for a
detailed list of the changes, with links to the relevant pull-requests,
discussions and documentation.

Regarding new features introduced, I just want to highlight here a tiny
subset of them:

<!--truncate-->

### Pulsar functions in Kubernetes

It's now possible to use Kubernetes as the scheduler for Pulsar Functions.

When a Pulsar cluster is configured to use Kubernetes, submitting a
function, using CLI tools or REST API, will cause the function instances
to be submitted as Kubernetes pods rather than running as processes
or threads within the Pulsar functions worker.

With this runtime manager, it's possible to set quota on CPU/Mem and
have Kubernetes assign the required resources and enforce isolation
between different instances and functions.

###  New Pulsar IO connectors:

A new batch of connectors was added, including MongoDB, Elastic Search,
HBase and local files source and sink.

We introduce support for doing [Change-Data-Capture](https://en.wikipedia.org/wiki/Change_data_capture)
with [Debezium](https://debezium.io/). This allows to record all
the update from a database into a Pulsar topic and use it for replication,
streaming jobs, cache updating, etc..

With Pulsar IO, Debezium will run as a regular Pulsar IO source,
completely managed by Pulsar. Users can easily submit a Debezium
builtin connector to a Pulsar cluster and start feeding data
from a long list of supported databases like MySQL, MongoDB,
PostgreSQL, Oracle and SQL Server.

Check out the [Debezium connector](/docs/io-cdc) documentation for how
to get started in capturing database changes.

### Token Authentication

Token Authentication provides a very simple and secure method of authentication for Pulsar.
This is based on [JSON Web Tokens](https://jwt.io/).

With tokens authentication, a client only needs to provide a single credential, or "token", in the
form of an opaque string provided by either the system administrator or some automated service.

The Java code for a client using token authentication will look like:

```java

PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://broker.example.com:6650/")
    .authentication(
        AuthenticationFactory.token("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJKb2UifQ.ipevRNuRP6HflG8cFKnmUPtypruRC4fb1DWtoLL62SY")
    .build();

```

See [Client Authentication using tokens](/docs/security-token-client) for a complete walk through
and instructions on how to set it up and manage it.


### Schema support in Python client library

This feature adds a Python idiomatic way to declare the schema
of a producer or consumer and integrates directly with the Pulsar
schema registry.

```python

import pulsar
from pulsar.schema import *

class Example(Record):
    a = String()
    b = Integer()
    c = Boolean()

producer = client.create_producer(
                    topic='my-topic',
                    schema=AvroSchema(Example) )

producer.send(Example(a='Hello', b=1))

```

The above example will make the producer `Example` schema to be
validated by broker when we try to publish on `my-topic`. If the
topic has a schema that is incompatible, the producer creation will
fail.

Currently, the Python schema support Avro and JSON, in addition to
regular types like `str` and `bytes`.

The complete documentation is available at [Python schema](/docs/client-libraries-python/#schema).

### Function state API in Python

From 2.3.0, Python function can access the state in as similar
way as Java functions, through the context object.

```python

import pulsar

# The classic ExclamationFunction that appends an
# exclamation at the end of the input
class WordCountFunction(pulsar.Function):
    def process(self, input, context):
        for word in input.split():
            context.incr_counter(word, 1)
        return input + "!"

```

Available methods for state management in the context object are:

```python

def incr_counter(self, key, amount):
  ""incr the counter of a given key in the managed state""

def get_counter(self, key):
  """get the counter of a given key in the managed state"""

def put_state(self, key, value):
  """update the value of a given key in the managed state"""

def get_state(self, key):
  """get the value of a given key in the managed state"""

```

## Conclusion

Please [download](/download) Pulsar 2.3.0 and report feedback, issues or any comment into our mailing lists,
slack channel or Github page. ([Contact page](/contact))
