---
id: io-debug
title: How To Debug Pulsar Connectors
sidebar_label: Debug
---

You can use the following methods to debug Pulsar Source or Sink:

* [Debug with localrun mode](io-debug.md#debug-with-localrun-mode)
* [Use Source or Sink CLI](io-debug.md#use-connectors-cli)
* [Analysis log](io-debug.md#analysis-log)

This article takes Mongo Sink as an example to introduce the debugging process of IO.

## Environmental preparation

1. Start Mongo service

```bash
docker pull mongo:4
docker run -d -p 27017:27017 --name pulsar-mongo -v $PWD/data:/data/db mongo:4
```

2. Create db and collection

```bash
docker exec -it pulsar-mongo /bin/bash
mongo
> use pulsar
> db.createCollection('messages')
> exit
```

3. Start Pulsar standalone

```bash
docker pull apachepulsar/pulsar:2.4.0
docker run -d -it -p 6650:6650 -p 8080:8080 -v $PWD/data:/pulsar/data --link pulsar-mongo --name pulsar-mongo-standalone apachepulsar/pulsar:2.4.0 bin/pulsar standalone
```

4. Configuration file mongo-sink-config.yaml

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

5. Download Mongo Sink nar package.

```bash
docker exec -it pulsar-mongo-standalone /bin/bash
curl -O http://apache.01link.hk/pulsar/pulsar-2.4.0/connectors/pulsar-io-mongo-2.4.0.nar
```

## Debug with localrun mode

Start Mongo Sink in localrun.

```bash
./bin/pulsar-admin sinks localrun --archive pulsar-io-mongo-2.4.0.nar --tenant public --namespace default --inputs test-mongo --name pulsar-mongo-sink --sink-config-file mongo-sink-config.yaml --parallelism 1
```
The following information will be printed out.
This information shows the storage path of nar package after decompression.
Sometimes when the thrown class cannot be found, you can look at whether there is code under the folder.

```bash
08:21:54.132 [main] INFO  org.apache.pulsar.common.nar.NarClassLoader - Created class loader with paths: [file:/tmp/pulsar-nar/pulsar-io-mongo-2.4.0.nar-unpacked/, file:/tmp/pulsar-nar/pulsar-io-mongo-2.4.0.nar-unpacked/META-INF/bundled-dependencies/,
```

This information shows some basic information about this Sink, such as which tenant it belongs to, namespace, name of Sink used, usage of configuration information resources, etc.
This information can be used to check whether your Sink is configured correctly.

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

This information shows the status of connection to mongo and some configuration information.

```bash
08:21:56.231 [cluster-ClusterId{value='5d6396a3c9e77c0569ff00eb', description='null'}-pulsar-mongo:27017] INFO  org.mongodb.driver.connection - Opened connection [connectionId{localValue:1, serverValue:8}] to pulsar-mongo:27017
08:21:56.326 [cluster-ClusterId{value='5d6396a3c9e77c0569ff00eb', description='null'}-pulsar-mongo:27017] INFO  org.mongodb.driver.cluster - Monitor thread successfully connected to server with description ServerDescription{address=pulsar-mongo:27017, type=STANDALONE, state=CONNECTED, ok=true, version=ServerVersion{versionList=[4, 2, 0]}, minWireVersion=0, maxWireVersion=8, maxDocumentSize=16777216, logicalSessionTimeoutMinutes=30, roundTripTimeNanos=89058800}
```

This information shows some configuration information of consumers and clients, including the Topic name used, subscription name, subscription type, whether the client has turned on authentication, etc.

```bash
08:21:56.719 [pulsar-client-io-1-1] INFO  org.apache.pulsar.client.impl.ConsumerStatsRecorderImpl - Starting Pulsar consumer perf with config: {
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

This is the process of running Sink in localrun mode, through testing in localrun mode, the problem can be located very quickly. this is a very useful tool.

## Use Sink CLI
Localrun is very convenient for local debugging, but it is not very suitable in the generation environment.
Let's take Mong Sink as an example to look at debugging in cluster mode.

Start Mongo Sink.

```bash
./bin/pulsar-admin sinks create --archive pulsar-io-mongo-2.4.0.nar --tenant public --namespace default --inputs test-mongo --name pulsar-mongo-sink --sink-config-file mongo-sink-config.yaml --parallelism 1
```

### Use the get command to get the basic information of Sink

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

### Use the status command to get the running status of Sink

This command can be used to obtain the confidence of the Sink's running status, instance id, workerId, etc.
WorkerId can help us locate the specific worker on which Sink is running when there are multiple workers.

```bash
./bin/pulsar-admin sinks status --tenant public --namespace default  --name pulsar-mongo-sink
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

### Get statistics on topic by using the topic stats command 

The `topics stats` is also a very useful command. Through this command, we can see in more detail whether our Sink is running normally, whether the topic has received messages, whether there is a backlog of messages, and the availablePermits and other key information.
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


## Analysis log
In some cases, problems cannot be located by localrun or by client commands. At this time, problems can be found more quickly by analyzing and viewing logs.

In localrun mode, the log location is located at  

```bash
logs/functions/public/default/pulsar-mongo-sink/pulsar-mongo-sink-0.log
```

The meaning of each field in this path is as follows:
```bash
logs/functions/tenant/namespace/function-name/function-name-instance-id.log
```

In cluster mode, the log location is located on a Worker and can be located by the `workerId` above.

The specific contents of the log are not detailed here, but can be analyzed in the above localrun mode.

## Summary
This article mainly introduces how to debug Sink. The same steps can be used for Source debugging.
The general process is summarized as follows:

1. Check whether the Pulsar Service starts normally.
2. Check whether the external service is normal.
3. Check whether nar package is complete, such as check sum is consistent, and whether it can run normally.
4. Check that the Source or Sink configuration file is correct.
5. Run Source or Sink in the localrun mode while observing the output information.
6. In cluster mode, command `sinks/sources get` is used to obtain operation information, `sinks/sources` status is used to obtain operation status, and `topics stats` is used to obtain statistical information of topics
7. Analysis log.
8. Enter the external system and verify the results.

