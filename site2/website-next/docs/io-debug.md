---
id: io-debug
title: How to debug Pulsar connectors
sidebar_label: Debug
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide explains how to debug connectors in localrun or cluster mode and gives a debugging checklist.
To better demonstrate how to debug Pulsar connectors, here takes a Mongo sink connector as an example.   

**Deploy a Mongo sink environment**
1. Start a Mongo service.
    ```bash
    docker pull mongo:4
    docker run -d -p 27017:27017 --name pulsar-mongo -v $PWD/data:/data/db mongo:4
    ```
2. Create a DB and a collection.
    ```bash
    docker exec -it pulsar-mongo /bin/bash
    mongo
    :::note

    use pulsar
    db.createCollection('messages')
    exit

    :::

    ```
3. Start Pulsar standalone.
    ```bash
    docker pull apachepulsar/pulsar:2.4.0
    docker run -d -it -p 6650:6650 -p 8080:8080 -v $PWD/data:/pulsar/data --link pulsar-mongo --name pulsar-mongo-standalone apachepulsar/pulsar:2.4.0 bin/pulsar standalone
    ```
4. Configure the Mongo sink with the `mongo-sink-config.yaml` file.
    ```bash
    configs:
    mongoUri: "mongodb://pulsar-mongo:27017"
    database: "pulsar"
    collection: "messages"
    batchSize: 2
    batchTimeMs: 500
    ```
    ```bash
    docker cp mongo-sink-config.yaml pulsar-mongo-standalone:/pulsar/
    ```
5. Download the Mongo sink nar package.
    ```bash
    docker exec -it pulsar-mongo-standalone /bin/bash
    curl -O http://apache.01link.hk/pulsar/pulsar-2.4.0/connectors/pulsar-io-mongo-2.4.0.nar
    ```
## Debug in localrun mode
Start the Mongo sink in localrun mode using the `localrun` command.
:::tip

For more information about the `localrun` command, see [`localrun`](reference-connector-admin.md/#localrun-1).

:::

```bash
./bin/pulsar-admin sinks localrun \
--archive pulsar-io-mongo-2.4.0.nar \ 
--tenant public --namespace default \
--inputs test-mongo \
--name pulsar-mongo-sink \
--sink-config-file mongo-sink-config.yaml \
--parallelism 1
```
### Use connector log
Use one of the following methods to get a connector log in localrun mode:
* After executing the `localrun` command, the **log is automatically printed on the console**.
* The log is located at:
  
    ```bash
    logs/functions/tenant/namespace/function-name/function-name-instance-id.log
    ```
    
    **Example**
    
    The path of the Mongo sink connector is:
    ```bash
    logs/functions/public/default/pulsar-mongo-sink/pulsar-mongo-sink-0.log
    ```
To clearly explain the log information, here breaks down the large block of information into small blocks and add descriptions for each block.
* This piece of log information shows the storage path of the nar package after decompression.
    ```
    08:21:54.132 [main] INFO  org.apache.pulsar.common.nar.NarClassLoader - Created class loader with paths: [file:/tmp/pulsar-nar/pulsar-io-mongo-2.4.0.nar-unpacked/, file:/tmp/pulsar-nar/pulsar-io-mongo-2.4.0.nar-unpacked/META-INF/bundled-dependencies/,
    ```
    :::tip

    If `class cannot be found` exception is thrown, check whether the nar file is decompressed in the folder `file:/tmp/pulsar-nar/pulsar-io-mongo-2.4.0.nar-unpacked/META-INF/bundled-dependencies/` or not.

    :::

* This piece of log information illustrates the basic information about the Mongo sink connector, such as tenant, namespace, name, parallelism, resources, and so on, which can be used to **check whether the Mongo sink connector is configured correctly or not**.
    ```bash
    08:21:55.390 [main] INFO  org.apache.pulsar.functions.runtime.ThreadRuntime - ThreadContainer starting function with instance config InstanceConfig(instanceId=0, functionId=853d60a1-0c48-44d5-9a5c-6917386476b2, functionVersion=c2ce1458-b69e-4175-88c0-a0a856a2be8c, functionDetails=tenant: "public"
    namespace: "default"
    name: "pulsar-mongo-sink"
    className: "org.apache.pulsar.functions.api.utils.IdentityFunction"
    autoAck: true
    parallelism: 1
    source {
    typeClassName: "[B"
    inputSpecs {
        key: "test-mongo"
        value {
        }
    }
    cleanupSubscription: true
    }
    sink {
    className: "org.apache.pulsar.io.mongodb.MongoSink"
    configs: "{\"mongoUri\":\"mongodb://pulsar-mongo:27017\",\"database\":\"pulsar\",\"collection\":\"messages\",\"batchSize\":2,\"batchTimeMs\":500}"
    typeClassName: "[B"
    }
    resources {
    cpu: 1.0
    ram: 1073741824
    disk: 10737418240
    }
    componentType: SINK
    , maxBufferedTuples=1024, functionAuthenticationSpec=null, port=38459, clusterName=local)
    ```
* This piece of log information demonstrates the status of the connections to Mongo and configuration information.
    ```bash
    08:21:56.231 [cluster-ClusterId{value='5d6396a3c9e77c0569ff00eb', description='null'}-pulsar-mongo:27017] INFO  org.mongodb.driver.connection - Opened connection [connectionId{localValue:1, serverValue:8}] to pulsar-mongo:27017
    08:21:56.326 [cluster-ClusterId{value='5d6396a3c9e77c0569ff00eb', description='null'}-pulsar-mongo:27017] INFO  org.mongodb.driver.cluster - Monitor thread successfully connected to server with description ServerDescription{address=pulsar-mongo:27017, type=STANDALONE, state=CONNECTED, ok=true, version=ServerVersion{versionList=[4, 2, 0]}, minWireVersion=0, maxWireVersion=8, maxDocumentSize=16777216, logicalSessionTimeoutMinutes=30, roundTripTimeNanos=89058800}
    ```
* This piece of log information explains the configuration of consumers and clients, including the topic name, subscription name, subscription type, and so on.
    ```bash
    08:21:56.719 [pulsar-client-io-1-1] INFO  org.apache.pulsar.client.impl.ConsumerStatsRecorderImpl - Starting Pulsar consumer status recorder with config: {
    "topicNames" : [ "test-mongo" ],
    "topicsPattern" : null,
    "subscriptionName" : "public/default/pulsar-mongo-sink",
    "subscriptionType" : "Shared",
    "receiverQueueSize" : 1000,
    "acknowledgementsGroupTimeMicros" : 100000,
    "negativeAckRedeliveryDelayMicros" : 60000000,
    "maxTotalReceiverQueueSizeAcrossPartitions" : 50000,
    "consumerName" : null,
    "ackTimeoutMillis" : 0,
    "tickDurationMillis" : 1000,
    "priorityLevel" : 0,
    "cryptoFailureAction" : "CONSUME",
    "properties" : {
        "application" : "pulsar-sink",
        "id" : "public/default/pulsar-mongo-sink",
        "instance_id" : "0"
    },
    "readCompacted" : false,
    "subscriptionInitialPosition" : "Latest",
    "patternAutoDiscoveryPeriod" : 1,
    "regexSubscriptionMode" : "PersistentOnly",
    "deadLetterPolicy" : null,
    "autoUpdatePartitions" : true,
    "replicateSubscriptionState" : false,
    "resetIncludeHead" : false
    }
    08:21:56.726 [pulsar-client-io-1-1] INFO  org.apache.pulsar.client.impl.ConsumerStatsRecorderImpl - Pulsar client config: {
    "serviceUrl" : "pulsar://localhost:6650",
    "authPluginClassName" : null,
    "authParams" : null,
    "operationTimeoutMs" : 30000,
    "statsIntervalSeconds" : 60,
    "numIoThreads" : 1,
    "numListenerThreads" : 1,
    "connectionsPerBroker" : 1,
    "useTcpNoDelay" : true,
    "useTls" : false,
    "tlsTrustCertsFilePath" : null,
    "tlsAllowInsecureConnection" : false,
    "tlsHostnameVerificationEnable" : false,
    "concurrentLookupRequest" : 5000,
    "maxLookupRequest" : 50000,
    "maxNumberOfRejectedRequestPerConnection" : 50,
    "keepAliveIntervalSeconds" : 30,
    "connectionTimeoutMs" : 10000,
    "requestTimeoutMs" : 60000,
    "defaultBackoffIntervalNanos" : 100000000,
    "maxBackoffIntervalNanos" : 30000000000
    }
    ```
## Debug in cluster mode
You can use the following methods to debug a connector in cluster mode:
* [Use connector log](#use-connector-log)
* [Use admin CLI](#use-admin-cli)
### Use connector log
In cluster mode, multiple connectors can run on a worker. To find the log path of a specified connector, use the `workerId` to locate the connector log.
### Use admin CLI
Pulsar admin CLI helps you debug Pulsar connectors with the following subcommands:
* [`get`](#get)
  
* [`status`](#status)
* [`topics stats`](#topics-stats)  

**Create a Mongo sink**
```bash
./bin/pulsar-admin sinks create \
--archive pulsar-io-mongo-2.4.0.nar \
--tenant public \
--namespace default \
--inputs test-mongo \
--name pulsar-mongo-sink \
--sink-config-file mongo-sink-config.yaml \
--parallelism 1
```
### `get`
Use the `get` command to get the basic information about the Mongo sink connector, such as tenant, namespace, name, parallelism, and so on.
```bash
./bin/pulsar-admin sinks get --tenant public --namespace default  --name pulsar-mongo-sink
{
  "tenant": "public",
  "namespace": "default",
  "name": "pulsar-mongo-sink",
  "className": "org.apache.pulsar.io.mongodb.MongoSink",
  "inputSpecs": {
    "test-mongo": {
      "isRegexPattern": false
    }
  },
  "configs": {
    "mongoUri": "mongodb://pulsar-mongo:27017",
    "database": "pulsar",
    "collection": "messages",
    "batchSize": 2.0,
    "batchTimeMs": 500.0
  },
  "parallelism": 1,
  "processingGuarantees": "ATLEAST_ONCE",
  "retainOrdering": false,
  "autoAck": true
}
```
:::tip

For more information about the `get` command, see [`get`](reference-connector-admin.md/#get-1).

:::

### `status`
Use the `status` command to get the current status about the Mongo sink connector, such as the number of instance, the number of running instance, instanceId, workerId and so on.
```bash
./bin/pulsar-admin sinks status 
--tenant public \
--namespace default  \
--name pulsar-mongo-sink
{
"numInstances" : 1,
"numRunning" : 1,
"instances" : [ {
    "instanceId" : 0,
    "status" : {
    "running" : true,
    "error" : "",
    "numRestarts" : 0,
    "numReadFromPulsar" : 0,
    "numSystemExceptions" : 0,
    "latestSystemExceptions" : [ ],
    "numSinkExceptions" : 0,
    "latestSinkExceptions" : [ ],
    "numWrittenToSink" : 0,
    "lastReceivedTime" : 0,
    "workerId" : "c-standalone-fw-5d202832fd18-8080"
    }
} ]
}
```
:::tip

For more information about the `status` command, see [`status`](reference-connector-admin.md/#stauts-1).

If there are multiple connectors running on a worker, `workerId` can locate the worker on which the specified connector is running.

:::

### `topics stats`
Use the `topics stats` command to get the stats for a topic and its connected producer and consumer, such as whether the topic has received messages or not, whether there is a backlog of messages or not, the available permits and other key information. All rates are computed over a 1-minute window and are relative to the last completed 1-minute period.
```bash
./bin/pulsar-admin topics stats test-mongo
{
  "msgRateIn" : 0.0,
  "msgThroughputIn" : 0.0,
  "msgRateOut" : 0.0,
  "msgThroughputOut" : 0.0,
  "averageMsgSize" : 0.0,
  "storageSize" : 1,
  "publishers" : [ ],
  "subscriptions" : {
    "public/default/pulsar-mongo-sink" : {
      "msgRateOut" : 0.0,
      "msgThroughputOut" : 0.0,
      "msgRateRedeliver" : 0.0,
      "msgBacklog" : 0,
      "blockedSubscriptionOnUnackedMsgs" : false,
      "msgDelayed" : 0,
      "unackedMessages" : 0,
      "type" : "Shared",
      "msgRateExpired" : 0.0,
      "consumers" : [ {
        "msgRateOut" : 0.0,
        "msgThroughputOut" : 0.0,
        "msgRateRedeliver" : 0.0,
        "consumerName" : "dffdd",
        "availablePermits" : 999,
        "unackedMessages" : 0,
        "blockedConsumerOnUnackedMsgs" : false,
        "metadata" : {
          "instance_id" : "0",
          "application" : "pulsar-sink",
          "id" : "public/default/pulsar-mongo-sink"
        },
        "connectedSince" : "2019-08-26T08:48:07.582Z",
        "clientVersion" : "2.4.0",
        "address" : "/172.17.0.3:57790"
      } ],
      "isReplicated" : false
    }
  },
  "replication" : { },
  "deduplicationStatus" : "Disabled"
}
```
:::tip

For more information about the `topic stats` command, see [`topic stats`](http://pulsar.apache.org/docs/en/pulsar-admin/#stats-1).

:::

## Checklist
This checklist indicates the major areas to check when you debug connectors. It is a reminder of what to look for to ensure a thorough review and an evaluation tool to get the status of connectors. 
* Does Pulsar start successfully?
  
* Does the external service run normally?
  
* Is the nar package complete?
  
* Is the connector configuration file correct?
  
* In localrun mode, run a connector and check the printed information (connector log) on the console.
  
* In cluster mode：
  
   * Use the `get` command to get the basic information.
  
   * Use the `status` command to get the current status.
   * Use the `topics stats` command to get the stats for a specified topic and its connected producers and consumers.
  
   * Check the connector log.
* Enter into the external system and verify the result.
