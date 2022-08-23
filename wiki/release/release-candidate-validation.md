<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Release Candidate Validation

- [Validate Binary Distribution](#validate-the-binary-distribution)
    * [Download And Verify Binary Distributions](#download-the-binary-distributions)
    * [Validate Pub/Sub and Java Functions](#validate-pubsub-and-java-functions)
    * [Validate Connectors](#validate-connectors)
    * [Validate Stateful Functions](#validate-stateful-functions)

The following are manual instructions for reviewing and validating a release candidate.
You can automate these steps. Contributions are welcome!

## Validate the binary distribution

### Download And Verify the binary distributions

Download the server distribution `apache-pulsar-<release>-bin.tar.gz` and extract it. The extracted files are in a directory called `apache-pulsar-<release>`. All the operations below happen within that directory.

```shell
$ cd apache-pulsar-<release>
$ mkdir connectors
```

Download the Pulsar IO Connector files
```
pulsar-io-aerospike-<release>.nar
pulsar-io-cassandra-<release>.nar
pulsar-io-kafka-<release>.nar
pulsar-io-kinesis-<release>.nar
pulsar-io-rabbitmq-<release>.nar
pulsar-io-twitter-<release>.nar
```
and place them in the `connectors` directory.

Download the `*.asc` file and verify the GPG signature

```bash
gpg verify apache-pulsar-<release>-bin.tar.gz.asc
```

### Validate Pub/Sub and Java Functions

1. Open a terminal to start a standalone cluster.

```shell
$ bin/pulsar standalone
```

When you start a standalone cluster, there are a few things to check.

a) The standalone cluster is able to locate all the connectors. The following logging information should be displayed.

```shell
Found connector ConnectorDefinition(name=kinesis, description=Kinesis sink connector, sourceClass=null, sinkClass=org.apache.pulsar.io.kinesis.KinesisSink) from /Users/sijie/tmp/apache-pulsar-2.1.0-incubating/./connectors/pulsar-io-kinesis-2.1.0-incubating.nar
...
Found connector ConnectorDefinition(name=cassandra, description=Writes data into Cassandra, sourceClass=null, sinkClass=org.apache.pulsar.io.cassandra.CassandraStringSink) from /Users/sijie/tmp/apache-pulsar-2.1.0-incubating/./connectors/pulsar-io-cassandra-2.1.0-incubating.nar
...
Found connector ConnectorDefinition(name=aerospike, description=Aerospike database sink, sourceClass=null, sinkClass=org.apache.pulsar.io.aerospike.AerospikeStringSink) from /Users/sijie/tmp/apache-pulsar-2.1.0-incubating/./connectors/pulsar-io-aerospike-2.1.0-incubating.nar
```

b) (since Pulsar 2.1 release) The standalone starts bookkeeper table service. The output is similar as follows:

```shell
12:12:26.099 [main] INFO  org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble - 'default' namespace for table service : namespace_name: "default"
default_stream_conf {
  key_type: HASH
  min_num_ranges: 24
  initial_num_ranges: 24
  split_policy {
    fixed_range_policy {
      num_ranges: 2
    }
  }
  rolling_policy {
    size_policy {
      max_segment_size: 134217728
    }
  }
  retention_policy {
    time_policy {
      retention_minutes: -1
    }
  }
}
```

c) Functions worker is started correctly. The output is similar as follows:

```shell
14:28:24.101 [main] INFO  org.apache.pulsar.functions.worker.WorkerService - Starting worker c-standalone-fw-localhost-8080...
14:28:24.907 [main] INFO  org.apache.pulsar.functions.worker.WorkerService - Worker Configs: {
  "workerId" : "c-standalone-fw-localhost-8080",
  "workerHostname" : "localhost",
  "workerPort" : 8080,
  "workerPortTls" : 6751,
  "jvmGCMetricsLoggerClassName" : null,
  "numHttpServerThreads" : 8,
  "connectorsDirectory" : "./connectors",
  "functionMetadataTopicName" : "metadata",
  "functionWebServiceUrl" : null,
  "pulsarServiceUrl" : "pulsar://127.0.0.1:6650",
  "pulsarWebServiceUrl" : "http://127.0.0.1:8080",
  "clusterCoordinationTopicName" : "coordinate",
  "pulsarFunctionsNamespace" : "public/functions",
  "pulsarFunctionsCluster" : "standalone",
  "numFunctionPackageReplicas" : 1,
  "downloadDirectory" : "/tmp/pulsar_functions",
  "stateStorageServiceUrl" : "bk://127.0.0.1:4181",
  "functionAssignmentTopicName" : "assignments",
  "schedulerClassName" : "org.apache.pulsar.functions.worker.scheduler.RoundRobinScheduler",
  "failureCheckFreqMs" : 30000,
  "rescheduleTimeoutMs" : 60000,
  "initialBrokerReconnectMaxRetries" : 60,
  "assignmentWriteMaxRetries" : 60,
  "instanceLivenessCheckFreqMs" : 30000,
  "clientAuthenticationPlugin" : null,
  "clientAuthenticationParameters" : null,
  "topicCompactionFrequencySec" : 1800,
  "tlsEnabled" : true,
  "tlsCertificateFilePath" : null,
  "tlsKeyFilePath" : null,
  "tlsTrustCertsFilePath" : null,
  "tlsAllowInsecureConnection" : false,
  "tlsRequireTrustedClientCertOnConnect" : false,
  "useTls" : false,
  "tlsHostnameVerificationEnable" : false,
  "authenticationEnabled" : false,
  "authenticationProviders" : null,
  "authorizationEnabled" : false,
  "superUserRoles" : null,
  "properties" : { },
  "threadContainerFactory" : null,
  "processContainerFactory" : {
    "javaInstanceJarLocation" : null,
    "pythonInstanceLocation" : null,
    "logDirectory" : null,
    "extraFunctionDependenciesDir" : null
  },
  "kubernetesContainerFactory" : null,
  "secretsProviderConfiguratorClassName" : null,
  "secretsProviderConfiguratorConfig" : null,
  "functionInstanceMinResources" : null,
  "workerWebAddress" : "http://localhost:8080",
  "functionMetadataTopic" : "persistent://public/functions/metadata",
  "clusterCoordinationTopic" : "persistent://public/functions/coordinate",
  "functionAssignmentTopic" : "persistent://public/functions/assignments"
}
```

d) Do sanity check before moving to the next step.

```shell
// check pulsar binary port is listened correctly
$ netstat -an | grep 6650 | grep LISTEN

// check function cluster
$ curl -s http://localhost:8080/admin/v2/worker/cluster
// example output
[{"workerId":"c-standalone-fw-localhost-6750","workerHostname":"localhost","port":6750}]

// check brokers
$ curl -s http://localhost:8080/admin/v2/namespaces/public
// example outoupt
["public/default","public/functions"]

// check connectors
$ curl -s http://localhost:8080/admin/v2/functions/connectors
// example output
[{"name":"aerospike","description":"Aerospike database sink","sinkClass":"org.apache.pulsar.io.aerospike.AerospikeStringSink"},{"name":"cassandra","description":"Writes data into Cassandra","sinkClass":"org.apache.pulsar.io.cassandra.CassandraStringSink"},{"name":"kafka","description":"Kafka source and sink connector","sourceClass":"org.apache.pulsar.io.kafka.KafkaStringSource","sinkClass":"org.apache.pulsar.io.kafka.KafkaStringSink"},{"name":"kinesis","description":"Kinesis sink connector","sinkClass":"org.apache.pulsar.io.kinesis.KinesisSink"},{"name":"rabbitmq","description":"RabbitMQ source connector","sourceClass":"org.apache.pulsar.io.rabbitmq.RabbitMQSource"},{"name":"twitter","description":"Ingest data from Twitter firehose","sourceClass":"org.apache.pulsar.io.twitter.TwitterFireHose"}]

// check table services
$ nc -vz4 localhost 4181
```

2. Open another terminal to submit a Java Exclamation function.

a) Create tenant and namespace.

```shell
$ bin/pulsar-admin tenants create test
$ bin/pulsar-admin namespaces create test/test-namespace
```

b) Create function.

```shell
$ bin/pulsar-admin functions create --function-config-file examples/example-function-config.yaml --jar examples/api-examples.jar
```

The following information is returned.
`Created Successfully`

3. At the same terminal as step 2, retrieve the function configuration.

```shell
$ bin/pulsar-admin functions get --tenant test --namespace test-namespace --name example
```

The output is similar as follows:

```shell
{
  "tenant": "test",
  "namespace": "test-namespace",
  "name": "example",
  "className": "org.apache.pulsar.functions.api.examples.ExclamationFunction",
  "userConfig": "{\"PublishTopic\":\"test_result\"}",
  "autoAck": true,
  "parallelism": 1,
  "source": {
    "topicsToSerDeClassName": {
      "test_src": ""
    },
    "typeClassName": "java.lang.String"
  },
  "sink": {
    "topic": "test_result",
    "typeClassName": "java.lang.String"
  },
  "resources": {}
}
```

4. At the same terminal as step 3, retrieve the function status.

```shell
$ bin/pulsar-admin functions status --tenant test --namespace test-namespace --name example
```
The output is similar as follows:

```shell
{
  "numInstances" : 1,
  "numRunning" : 1,
  "instances" : [ {
    "instanceId" : 0,
    "status" : {
      "running" : true,
      "error" : "",
      "numRestarts" : 0,
      "numReceived" : 0,
      "numSuccessfullyProcessed" : 0,
      "numUserExceptions" : 0,
      "latestUserExceptions" : [ ],
      "numSystemExceptions" : 0,
      "latestSystemExceptions" : [ ],
      "averageLatency" : 0.0,
      "lastInvocationTime" : 0,
      "workerId" : "c-standalone-fw-localhost-8080"
    }
  } ]
}
```

5. At the same terminal as step 4, subscribe the output topic `test_result`.

```shell
$ bin/pulsar-client consume -s test-sub -n 0 test_result
```

6. Open a new terminal to produce messages into the input topic `test_src`.

```shell
$ bin/pulsar-client produce -m "test-messages-`date`" -n 10 test_src
```

7. At the terminal of step 5, the messages produced by the Exclamation function is returned. The output is similar as follows:

```shell
----- got message -----
test-messages-Thu Jul 19 11:59:15 PDT 2018!
----- got message -----
test-messages-Thu Jul 19 11:59:15 PDT 2018!
----- got message -----
test-messages-Thu Jul 19 11:59:15 PDT 2018!
----- got message -----
test-messages-Thu Jul 19 11:59:15 PDT 2018!
----- got message -----
test-messages-Thu Jul 19 11:59:15 PDT 2018!
----- got message -----
test-messages-Thu Jul 19 11:59:15 PDT 2018!
----- got message -----
test-messages-Thu Jul 19 11:59:15 PDT 2018!
----- got message -----
test-messages-Thu Jul 19 11:59:15 PDT 2018!
----- got message -----
test-messages-Thu Jul 19 11:59:15 PDT 2018!
----- got message -----
test-messages-Thu Jul 19 11:59:15 PDT 2018!
```

### Validate Connectors

> Make sure you have docker available at your laptop. If you haven't installed docker, you can skip this section.

1. Set up a cassandra cluster.

```shell
$ docker run -d --rm  --name=cassandra -p 9042:9042 cassandra
```

Make sure that the cassandra cluster is running.

```shell
// run docker ps to find the docker process for cassandra
$ docker ps
```

```shell
// check if the cassandra is running as expected
$ docker logs cassandra
```

```shell
// check the cluster status
$ docker exec cassandra nodetool status
Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address     Load       Tokens       Owns (effective)  Host ID                               Rack
UN  172.17.0.2  103.67 KiB  256          100.0%            af0e4b2f-84e0-4f0b-bb14-bd5f9070ff26  rack1
```

2. Create keyspace and table.

Run cqlsh:
```shell
$ docker exec -ti cassandra cqlsh localhost
Connected to Test Cluster at localhost:9042.
[cqlsh 5.0.1 | Cassandra 3.11.2 | CQL spec 3.4.4 | Native protocol v4]
Use HELP for help.
cqlsh>
```

In the cqlsh, create the `pulsar_test_keyspace` keyspace and the `pulsar_test_table` table.

```shell
cqlsh> CREATE KEYSPACE pulsar_test_keyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};
cqlsh> USE pulsar_test_keyspace;
cqlsh:pulsar_test_keyspace> CREATE TABLE pulsar_test_table (key text PRIMARY KEY, col text);

```

3. Prepare a cassandra sink yaml file and put it under examples directory as `cassandra-sink.yml`.

```shell
$ cat examples/cassandra-sink.yml
configs:
    roots: "localhost:9042"
    keyspace: "pulsar_test_keyspace"
    columnFamily: "pulsar_test_table"
    keyname: "key"
    columnName: "col"
```

4. Submit a cassandra sink.

```shell
$ bin/pulsar-admin sink create --tenant public --namespace default --name cassandra-test-sink --sink-type cassandra --sink-config-file examples/cassandra-sink.yml --inputs test_cassandra
"Created successfully"
```

```shell
// get the sink info
$ bin/pulsar-admin sink get --tenant public --namespace default --name cassandra-test-sink
{
  "tenant": "public",
  "namespace": "default",
  "name": "cassandra-test-sink",
  "className": "org.apache.pulsar.io.cassandra.CassandraStringSink",
  "inputSpecs": {
    "test_cassandra": {
      "isRegexPattern": false
    }
  },
  "configs": {
    "roots": "localhost:9042",
    "keyspace": "pulsar_test_keyspace",
    "columnFamily": "pulsar_test_table",
    "keyname": "key",
    "columnName": "col"
  },
  "parallelism": 1,
  "processingGuarantees": "ATLEAST_ONCE",
  "retainOrdering": false,
  "autoAck": true,
  "archive": "builtin://cassandra"
}
```

```shell
// get the running status
$ bin/pulsar-admin sink status --tenant public --namespace default --name cassandra-test-sink
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
      "workerId" : "c-standalone-fw-localhost-8080"
    }
  } ]
}
```

5. Produce messages to the source topic.
```shell
$ for i in {0..10}; do bin/pulsar-client produce -m "key-$i" -n 1 test_cassandra; done
```

6. Check the sink status, and 11 messages are processed.

```shell
$ bin/pulsar-admin sink status --tenant public --namespace default --name cassandra-test-sink
{
  "numInstances" : 1,
  "numRunning" : 1,
  "instances" : [ {
    "instanceId" : 0,
    "status" : {
      "running" : true,
      "error" : "",
      "numRestarts" : 0,
      "numReadFromPulsar" : 11,
      "numSystemExceptions" : 0,
      "latestSystemExceptions" : [ ],
      "numSinkExceptions" : 0,
      "latestSinkExceptions" : [ ],
      "numWrittenToSink" : 11,
      "lastReceivedTime" : 1554833501277,
      "workerId" : "c-standalone-fw-localhost-8080"
    }
  } ]
}
```

7. Check results in cassandra.

```shell
$ docker exec -ti cassandra cqlsh localhost
Connected to Test Cluster at localhost:9042.
[cqlsh 5.0.1 | Cassandra 3.11.2 | CQL spec 3.4.4 | Native protocol v4]
Use HELP for help.
cqlsh> use pulsar_test_keyspace;
cqlsh:pulsar_test_keyspace> select * from pulsar_test_table;

 key    | col
--------+--------
  key-5 |  key-5
  key-0 |  key-0
  key-9 |  key-9
  key-2 |  key-2
  key-1 |  key-1
  key-3 |  key-3
  key-6 |  key-6
  key-7 |  key-7
  key-4 |  key-4
  key-8 |  key-8
 key-10 | key-10

(11 rows)
```

8. Delete the sink.

```shell
$ bin/pulsar-admin sink delete --tenant public --namespace default --name cassandra-test-sink
"Deleted successfully"
```

### Validate Stateful Functions

Since Pulsar 2.1 release, Pulsar enables bookkeeper table service for stateful Pulsar functions (as a developer preview).

The following are instructions to validate counter functions.

1. Create a wordcount function.

```shell
$ bin/pulsar-admin functions create --function-config-file examples/example-function-config.yaml --jar examples/api-examples.jar --name word_count --className org.apache.pulsar.functions.api.examples.WordCountFunction --inputs test_wordcount_src --output test_wordcount_dest
"Created successfully"
```

2. Get function information and status.

```shell
$ bin/pulsar-admin functions get --tenant test --namespace test-namespace --name word_count
{
  "tenant": "test",
  "namespace": "test-namespace",
  "name": "word_count",
  "className": "org.apache.pulsar.functions.api.examples.WordCountFunction",
  "inputSpecs": {
    "test_wordcount_src": {
      "isRegexPattern": false
    }
  },
  "output": "test_wordcount_dest",
  "processingGuarantees": "ATLEAST_ONCE",
  "retainOrdering": false,
  "userConfig": {
    "PublishTopic": "test_result"
  },
  "runtime": "JAVA",
  "autoAck": true,
  "parallelism": 1,
  "resources": {
    "cpu": 1.0,
    "ram": 1073741824,
    "disk": 10737418240
  },
  "cleanupSubscription": true
}
```

```shell
$ bin/pulsar-admin functions status --tenant test --namespace test-namespace --name word_count
{
  "numInstances" : 1,
  "numRunning" : 1,
  "instances" : [ {
    "instanceId" : 0,
    "status" : {
      "running" : true,
      "error" : "",
      "numRestarts" : 0,
      "numReceived" : 0,
      "numSuccessfullyProcessed" : 0,
      "numUserExceptions" : 0,
      "latestUserExceptions" : [ ],
      "numSystemExceptions" : 0,
      "latestSystemExceptions" : [ ],
      "averageLatency" : 0.0,
      "lastInvocationTime" : 0,
      "workerId" : "c-standalone-fw-localhost-8080"
    }
  } ]
}
```

3. Query the state table for the function: watching on a key called "hello"

```shell
$ bin/pulsar-admin functions querystate --tenant test --namespace test-namespace --name word_count -k hello -w
key 'hello' doesn't exist.
key 'hello' doesn't exist.
key 'hello' doesn't exist
```

4. Produce the messages to source topic `test_wordcount_src`.

Produce 10 messages "hello" to the `test_wordcount_src` topic. The value of "hello" is updated to 10.

```shell
$ bin/pulsar-client produce -m "hello" -n 10 test_wordcount_src
```

Checkout the result in the terminal of step 3.

```shell
{
  "key": "hello",
  "numberValue": 10,
  "version": 9
}
```

Produce another 10 messages "hello". The result is updated to 20.

```shell
$ bin/pulsar-client produce -m "hello" -n 10 test_wordcount_src
```

The result in the terminal of step 3 is updated to `20`.

```shell
  "key": "hello",
  "numberValue": 20,
  "version": 19
```
