---
id: version-2.2.1-standalone-docker
title: Start a standalone cluster with Docker
sidebar_label: Pulsar in Docker
original_id: standalone-docker
---

For the purposes of local development and testing, you can run Pulsar in standalone
mode on your own machine within a Docker container.

If you don't have Docker installed, you can download the [Community edition](https://www.docker.com/community-edition)
and follow the instructions for your OS.

## Starting Pulsar inside Docker

```shell
$ docker run -it \
  -p 6650:6650 \
  -p 8080:8080 \
  -v $PWD/data:/pulsar/data \
  apachepulsar/pulsar:{{site.current_version}} \
  bin/pulsar standalone
```

Under Windows, you should use something like the following docker command:

```shell
$ docker run -it \
  -p 6650:6650 \
  -p 8080:8080 \
  -v "$PWD/data:/pulsar/data".ToLower() \
  apachepulsar/pulsar:{{site.current_version}} \
  bin/pulsar standalone
```

A few things to note about this command:
 * `$PWD/data` : The docker host directory under the Windows operating system must be lowercase.`$PWD/data` can provide you with the specified directory, for example: `E:/data`.
 * `-v $PWD/data:/pulsar/data`: This will make the process inside the container to store the
   data and metadata in the filesystem outside the container, in order to not start "fresh" every
   time the container is restarted.

If Pulsar has been successfully started, you should see `INFO`-level log messages like this:

```
2017-08-09 22:34:04,030 - INFO  - [main:WebService@213] - Web Service started at http://127.0.0.1:8080
2017-08-09 22:34:04,038 - INFO  - [main:PulsarService@335] - messaging service is ready, bootstrap service on port=8080, broker url=pulsar://127.0.0.1:6650, cluster=standalone, configs=org.apache.pulsar.broker.ServiceConfiguration@4db60246
...
```


> #### Automatically created namespace
> When you start a local standalone cluster, Pulsar will automatically create a `public/default`
namespace that you can use for development purposes. All Pulsar topics are managed within namespaces.
For more info, see [Topics](concepts-messaging.md#topics).


## Start publishing and consuming messages

Pulsar currently offers client libraries for [Java](client-libraries-java.md), [Go](client-libraries-go.md), [Python](client-libraries-python.md) 
and [C++](client-libraries-cpp.md). If you're running a local standalone cluster, you can
use one of these root URLs for interacting with your cluster:

* `pulsar://localhost:6650`
* `http://localhost:8080`

Here's an example that lets you quickly get started with Pulsar by using the [Python](client-libraries-python.md)
client API.

You can install the Pulsar Python client library directly from [PyPI](https://pypi.org/project/pulsar-client/):

```shell
$ pip install pulsar-client
```

First create a consumer and subscribe to the topic:

```python
import pulsar

client = pulsar.Client('pulsar://localhost:6650')
consumer = client.subscribe('my-topic',
                            subscription_name='my-sub')

while True:
    msg = consumer.receive()
    print("Received message: '%s'" % msg.data())
    consumer.acknowledge(msg)

client.close()
```

Now we can start a producer to send some test messages:

```python
import pulsar

client = pulsar.Client('pulsar://localhost:6650')
producer = client.create_producer('my-topic')

for i in range(10):
    producer.send(('hello-pulsar-%d' % i).encode('utf-8'))

client.close()
```


## Get the topic statistics

In Pulsar you can use REST, Java, or command-line tools to control every aspect of the system.
You can find detailed documentation of all the APIs in the [Admin API Overview](admin-api-overview.md).

In the simplest example, you can use curl to probe the stats for a particular topic:

```shell
$ curl http://localhost:8080/admin/v2/persistent/public/default/my-topic/stats | python -m json.tool
```

The output will be something like this:

```json
{
  "averageMsgSize": 0.0,
  "msgRateIn": 0.0,
  "msgRateOut": 0.0,
  "msgThroughputIn": 0.0,
  "msgThroughputOut": 0.0,
  "publishers": [
    {
      "address": "/172.17.0.1:35048",
      "averageMsgSize": 0.0,
      "clientVersion": "1.19.0-incubating",
      "connectedSince": "2017-08-09 20:59:34.621+0000",
      "msgRateIn": 0.0,
      "msgThroughputIn": 0.0,
      "producerId": 0,
      "producerName": "standalone-0-1"
    }
  ],
  "replication": {},
  "storageSize": 16,
  "subscriptions": {
    "my-sub": {
      "blockedSubscriptionOnUnackedMsgs": false,
      "consumers": [
        {
          "address": "/172.17.0.1:35064",
          "availablePermits": 996,
          "blockedConsumerOnUnackedMsgs": false,
          "clientVersion": "1.19.0-incubating",
          "connectedSince": "2017-08-09 21:05:39.222+0000",
          "consumerName": "166111",
          "msgRateOut": 0.0,
          "msgRateRedeliver": 0.0,
          "msgThroughputOut": 0.0,
          "unackedMessages": 0
        }
      ],
      "msgBacklog": 0,
      "msgRateExpired": 0.0,
      "msgRateOut": 0.0,
      "msgRateRedeliver": 0.0,
      "msgThroughputOut": 0.0,
      "type": "Exclusive",
      "unackedMessages": 0
    }
  }
}
```
