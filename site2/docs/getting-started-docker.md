---
id: getting-started-docker
title: Run a standalone Pulsar cluster in Docker
sidebar_label: "Run Pulsar in Docker"
---

For local development and testing, you can run Pulsar in standalone mode on your own machine within a Docker container.

If you have not installed Docker, download the [Community edition](https://www.docker.com/community-edition) and follow the instructions for your OS.

## Start Pulsar in Docker

For macOS, Linux, and Windows, run the following command to start Pulsar within a Docker container.

```shell
docker run -it -p 6650:6650  -p 8080:8080 --mount source=pulsardata,target=/pulsar/data --mount source=pulsarconf,target=/pulsar/conf apachepulsar/pulsar:@pulsar:version@ bin/pulsar standalone
```

If you want to change Pulsar configurations and start Pulsar, run the following command by passing environment variables with the `PULSAR_PREFIX_` prefix. See [default configuration file](https://github.com/apache/pulsar/blob/e6b12c64b043903eb5ff2dc5186fe8030f157cfc/conf/standalone.conf) for more details.

```shell
docker run -it -e PULSAR_PREFIX_xxx=yyy -p 6650:6650  -p 8080:8080 --mount source=pulsardata,target=/pulsar/data --mount source=pulsarconf,target=/pulsar/conf apachepulsar/pulsar:2.10.0 sh -c "bin/apply-config-from-env.py conf/standalone.conf && bin/pulsar standalone"
```

:::tip

* The docker container runs as UID 10000 and GID 0 by default. You need to ensure the mounted volumes give write permission to either UID 10000 or GID 0. Note that UID 10000 is arbitrary, so it is recommended to make these mounts writable for the root group (GID 0).
* The data, metadata, and configuration are persisted on Docker volumes to not start "fresh" every time the container is restarted. For details on the volumes, you can use `docker volume inspect <sourcename>`.
* For Docker on Windows, make sure to configure it to use Linux containers.

:::

After starting Pulsar successfully, you can see `INFO`-level log messages like this:

```
08:18:30.970 [main] INFO  org.apache.pulsar.broker.web.WebService - HTTP Service started at http://0.0.0.0:8080
...
07:53:37.322 [main] INFO  org.apache.pulsar.broker.PulsarService - messaging service is ready, bootstrap service port = 8080, broker url= pulsar://localhost:6650, cluster=standalone, configs=org.apache.pulsar.broker.ServiceConfiguration@98b63c1
...
```

:::tip

* To perform a health check, you can use the `bin/pulsar-admin brokers healthcheck` command. For more information, see [Pulsar-admin docs](/tools/pulsar-admin/).
* When you start a local standalone cluster, a `public/default` namespace is created automatically. The namespace is used for development purposes. All Pulsar topics are managed within namespaces. For more information, see [Topics](concepts-messaging.md#topics).

:::

## Use Pulsar in Docker

Pulsar offers a variety of [client libraries](client-libraries.md), such as [Java](client-libraries-java.md), [Go](client-libraries-go.md), [Python](client-libraries-python.md), [C++](client-libraries-cpp.md).

If you're running a local standalone cluster, you can use one of these root URLs to interact with your cluster:
* `pulsar://localhost:6650`
* `http://localhost:8080`

The following example guides you to get started with Pulsar by using the [Python client API](client-libraries-python.md).

Install the Pulsar Python client library directly from [PyPI](https://pypi.org/project/pulsar-client/):

```shell
pip install pulsar-client
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

Start a producer to send some test messages:

```python
import pulsar

client = pulsar.Client('pulsar://localhost:6650')
producer = client.create_producer('my-topic')

for i in range(10):
    producer.send(('hello-pulsar-%d' % i).encode('utf-8'))

client.close()
```

## Get the topic statistics

In Pulsar, you can use REST API, Java, or command-line tools to control every aspect of the system. For details on APIs, refer to [Admin API Overview](admin-api-overview.md).

In the simplest example, you can use curl to probe the stats for a particular topic:

```shell
curl http://localhost:8080/admin/v2/persistent/public/default/my-topic/stats | python -m json.tool
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

