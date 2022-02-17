---
id: standalone-docker
title: Set up a standalone Pulsar in Docker
sidebar_label: "Run Pulsar in Docker"
---

For local development and testing, you can run Pulsar in standalone mode on your own machine within a Docker container. 

If you have not installed Docker, download the [Community edition](https://www.docker.com/community-edition) and follow the instructions for your OS.

## Start Pulsar in Docker

* For MacOS, Linux, and Windows:

  ```shell
  
  $ docker run -it -p 6650:6650  -p 8080:8080 --mount source=pulsardata,target=/pulsar/data --mount source=pulsarconf,target=/pulsar/conf apachepulsar/pulsar:@pulsar:version@ bin/pulsar standalone
  
  ```

A few things to note about this command:
 * The data, metadata, and configuration are persisted on Docker volumes in order to not start "fresh" every 
time the container is restarted. For details on the volumes you can use `docker volume inspect <sourcename>`
 * For Docker on Windows make sure to configure it to use Linux containers

If you start Pulsar successfully, you will see `INFO`-level log messages like this:

```

08:18:30.970 [main] INFO  org.apache.pulsar.broker.web.WebService - HTTP Service started at http://0.0.0.0:8080
...
07:53:37.322 [main] INFO  org.apache.pulsar.broker.PulsarService - messaging service is ready, bootstrap service port = 8080, broker url= pulsar://localhost:6650, cluster=standalone, configs=org.apache.pulsar.broker.ServiceConfiguration@98b63c1
...

```

:::tip

When you start a local standalone cluster, a `public/default`

:::

namespace is created automatically. The namespace is used for development purposes. All Pulsar topics are managed within namespaces.
For more information, see [Topics](concepts-messaging.md#topics).

## Use Pulsar in Docker

Pulsar offers client libraries for [Java](client-libraries-java.md), [Go](client-libraries-go.md), [Python](client-libraries-python) 
and [C++](client-libraries-cpp). If you're running a local standalone cluster, you can
use one of these root URLs to interact with your cluster:

* `pulsar://localhost:6650`
* `http://localhost:8080`

The following example will guide you get started with Pulsar quickly by using the [Python client API](client-libraries-python)
client API.

Install the Pulsar Python client library directly from [PyPI](https://pypi.org/project/pulsar-client/):

```shell

$ pip install pulsar-client

```

### Consume a message

Create a consumer and subscribe to the topic:

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

### Produce a message

Now start a producer to send some test messages:

```python

import pulsar

client = pulsar.Client('pulsar://localhost:6650')
producer = client.create_producer('my-topic')

for i in range(10):
    producer.send(('hello-pulsar-%d' % i).encode('utf-8'))

client.close()

```

## Get the topic statistics

In Pulsar, you can use REST, Java, or command-line tools to control every aspect of the system.
For details on APIs, refer to [Admin API Overview](admin-api-overview).

In the simplest example, you can use curl to probe the stats for a particular topic:

```shell

$ curl http://localhost:8080/admin/v2/persistent/public/default/my-topic/stats | python -m json.tool

```

The output is something like this:

```json

{
    "msgRateIn": 0.0,
    "msgThroughputIn": 0.0,
    "msgRateOut": 1.8332950480217471,
    "msgThroughputOut": 91.33142602871978,
    "bytesInCounter": 7097,
    "msgInCounter": 143,
    "bytesOutCounter": 6607,
    "msgOutCounter": 133,
    "averageMsgSize": 0.0,
    "msgChunkPublished": false,
    "storageSize": 7097,
    "backlogSize": 0,
    "offloadedStorageSize": 0,
    "publishers": [
        {
            "accessMode": "Shared",
            "msgRateIn": 0.0,
            "msgThroughputIn": 0.0,
            "averageMsgSize": 0.0,
            "chunkedMessageRate": 0.0,
            "producerId": 0,
            "metadata": {},
            "address": "/127.0.0.1:35604",
            "connectedSince": "2021-07-04T09:05:43.04788Z",
            "clientVersion": "2.8.0",
            "producerName": "standalone-2-5"
        }
    ],
    "waitingPublishers": 0,
    "subscriptions": {
        "my-sub": {
            "msgRateOut": 1.8332950480217471,
            "msgThroughputOut": 91.33142602871978,
            "bytesOutCounter": 6607,
            "msgOutCounter": 133,
            "msgRateRedeliver": 0.0,
            "chunkedMessageRate": 0,
            "msgBacklog": 0,
            "backlogSize": 0,
            "msgBacklogNoDelayed": 0,
            "blockedSubscriptionOnUnackedMsgs": false,
            "msgDelayed": 0,
            "unackedMessages": 0,
            "type": "Exclusive",
            "activeConsumerName": "3c544f1daa",
            "msgRateExpired": 0.0,
            "totalMsgExpired": 0,
            "lastExpireTimestamp": 0,
            "lastConsumedFlowTimestamp": 1625389101290,
            "lastConsumedTimestamp": 1625389546070,
            "lastAckedTimestamp": 1625389546162,
            "lastMarkDeleteAdvancedTimestamp": 1625389546163,
            "consumers": [
                {
                    "msgRateOut": 1.8332950480217471,
                    "msgThroughputOut": 91.33142602871978,
                    "bytesOutCounter": 6607,
                    "msgOutCounter": 133,
                    "msgRateRedeliver": 0.0,
                    "chunkedMessageRate": 0.0,
                    "consumerName": "3c544f1daa",
                    "availablePermits": 867,
                    "unackedMessages": 0,
                    "avgMessagesPerEntry": 6,
                    "blockedConsumerOnUnackedMsgs": false,
                    "lastAckedTimestamp": 1625389546162,
                    "lastConsumedTimestamp": 1625389546070,
                    "metadata": {},
                    "address": "/127.0.0.1:35472",
                    "connectedSince": "2021-07-04T08:58:21.287682Z",
                    "clientVersion": "2.8.0"
                }
            ],
            "isDurable": true,
            "isReplicated": false,
            "allowOutOfOrderDelivery": false,
            "consumersAfterMarkDeletePosition": {},
            "nonContiguousDeletedMessagesRanges": 0,
            "nonContiguousDeletedMessagesRangesSerializedSize": 0,
            "durable": true,
            "replicated": false
        }
    },
    "replication": {},
    "deduplicationStatus": "Disabled",
    "nonContiguousDeletedMessagesRanges": 0,
    "nonContiguousDeletedMessagesRangesSerializedSize": 0
}

```

