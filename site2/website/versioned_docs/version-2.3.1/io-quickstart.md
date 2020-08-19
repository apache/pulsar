---
id: version-2.3.1-io-quickstart
title: "Tutorial: Connecting Pulsar with Apache Cassandra"
sidebar_label: Getting started
original_id: io-quickstart
---

This tutorial provides a hands-on look at how you can move data out of Pulsar without writing a single line of code.
It is helpful to review the [concepts](io-overview.md) for Pulsar I/O in tandem with running the steps in this guide
to gain a deeper understanding. At the end of this tutorial, you will be able to:

- Connect your Pulsar cluster with your Cassandra cluster

> #### Tip
>
> 1. These instructions assume you are running Pulsar in [standalone mode](getting-started-standalone.md). However all
> the commands used in this tutorial should be able to be used in a multi-nodes Pulsar cluster without any changes.
>
> 2. All the instructions are assumed to run at the root directory of a Pulsar binary distribution.

## Installing Pulsar

To get started running Pulsar, download a binary tarball release in one of the following ways:

* by clicking the link below and downloading the release from an Apache mirror:

  * <a href="pulsar:binary_release_url" download>Pulsar {{pulsar:version}} binary release</a>

* from the Pulsar [downloads page](pulsar:download_page_url)
* from the Pulsar [releases page](https://github.com/apache/pulsar/releases/latest)
* using [wget](https://www.gnu.org/software/wget):

  ```shell
  $ wget pulsar:binary_release_url
  ```

Once the tarball is downloaded, untar it and `cd` into the resulting directory:

```bash
$ tar xvfz apache-pulsar-{{pulsar:version}}-bin.tar.gz
$ cd apache-pulsar-{{pulsar:version}}
```

## Installing Builtin Connectors

Since release `2.3.0`, Pulsar releases all the `builtin` connectors as individual archives.
If you would like to enable those `builtin` connectors, you can download the connectors "NAR"
archives and from the Pulsar [downloads page](pulsar:download_page_url).

After downloading the desired builtin connectors, these archives should be places under
the `connectors` directory where you have unpacked the Pulsar distribution.


```bash
# Unpack regular Pulsar tarball and copy connectors NAR archives
$ tar xvfz /path/to/apache-pulsar-{{pulsar:version}}-bin.tar.gz
$ cd apache-pulsar-{{pulsar:version}}
$ mkdir connectors
$ cp -r /path/to/downloaded/connectors/*.nar ./connectors

$ ls connectors
pulsar-io-aerospike-{{pulsar:version}}.nar
pulsar-io-cassandra-{{pulsar:version}}.nar
pulsar-io-kafka-{{pulsar:version}}.nar
pulsar-io-kinesis-{{pulsar:version}}.nar
pulsar-io-rabbitmq-{{pulsar:version}}.nar
pulsar-io-twitter-{{pulsar:version}}.nar
...
```

> #### Tip
>
> You can also use the Docker image `apachepulsar/pulsar-all:{{pulsar:version}}` which already
> comes with all the available builtin connectors.

## Start Pulsar Service

```bash
bin/pulsar standalone
```

All the components of a Pulsar service will start in order. You can curl those pulsar service endpoints to make sure Pulsar service is up running correctly.

1. Check pulsar binary protocol port.

```bash
telnet localhost 6650
```

2. Check pulsar function cluster

```bash
curl -s http://localhost:8080/admin/v2/worker/cluster
```

Example output:
```shell
[{"workerId":"c-standalone-fw-localhost-6750","workerHostname":"localhost","port":6750}]
```

3. Make sure public tenant and default namespace exist

```bash
curl -s http://localhost:8080/admin/v2/namespaces/public
```

Example outoupt:
```shell
["public/default","public/functions"]
```

4. All builtin connectors should be listed as available.

```bash
curl -s http://localhost:8080/admin/v2/functions/connectors
```

Example output:
```json
[{"name":"aerospike","description":"Aerospike database sink","sinkClass":"org.apache.pulsar.io.aerospike.AerospikeStringSink"},{"name":"cassandra","description":"Writes data into Cassandra","sinkClass":"org.apache.pulsar.io.cassandra.CassandraStringSink"},{"name":"kafka","description":"Kafka source and sink connector","sourceClass":"org.apache.pulsar.io.kafka.KafkaStringSource","sinkClass":"org.apache.pulsar.io.kafka.KafkaBytesSink"},{"name":"kinesis","description":"Kinesis sink connector","sinkClass":"org.apache.pulsar.io.kinesis.KinesisSink"},{"name":"rabbitmq","description":"RabbitMQ source connector","sourceClass":"org.apache.pulsar.io.rabbitmq.RabbitMQSource"},{"name":"twitter","description":"Ingest data from Twitter firehose","sourceClass":"org.apache.pulsar.io.twitter.TwitterFireHose"}]
```

If an error occurred while starting Pulsar service, you may be able to seen exception at the terminal you are running `pulsar/standalone`,
or you can navigate the `logs` directory under the Pulsar directory to view the logs.

## Connect Pulsar to Apache Cassandra

> Make sure you have docker available at your laptop. If you don't have docker installed, you can follow the [instructions](https://docs.docker.com/docker-for-mac/install/).

We are using `cassandra` docker image to start a single-node cassandra cluster in Docker.

### Setup the Cassandra Cluster

#### Start a Cassandra Cluster

```bash
docker run -d --rm --name=cassandra -p 9042:9042 cassandra
```

Before moving to next steps, make sure the cassandra cluster is up running.

1. Make sure the docker process is running.

```bash
docker ps
```

2. Check the cassandra logs to make sure cassandra process is running as expected.

```bash
docker logs cassandra
```

3. Check the cluster status

```bash
docker exec cassandra nodetool status
```

Example output:
```
Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address     Load       Tokens       Owns (effective)  Host ID                               Rack
UN  172.17.0.2  103.67 KiB  256          100.0%            af0e4b2f-84e0-4f0b-bb14-bd5f9070ff26  rack1
```

#### Create keyspace and table

We are using `cqlsh` to connect to the cassandra cluster to create keyspace and table.

```bash
$ docker exec -ti cassandra cqlsh localhost
Connected to Test Cluster at localhost:9042.
[cqlsh 5.0.1 | Cassandra 3.11.2 | CQL spec 3.4.4 | Native protocol v4]
Use HELP for help.
cqlsh>
```

All the following commands are executed in `cqlsh`.

##### Create keyspace `pulsar_test_keyspace`

```bash
cqlsh> CREATE KEYSPACE pulsar_test_keyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};
```

#### Create table `pulsar_test_table`

```bash
cqlsh> USE pulsar_test_keyspace;
cqlsh:pulsar_test_keyspace> CREATE TABLE pulsar_test_table (key text PRIMARY KEY, col text);
```

### Configure a Cassandra Sink

Now that we have a Cassandra cluster running locally. In this section, we will configure a Cassandra sink connector.
The Cassandra sink connector will read messages from a Pulsar topic and write the messages into a Cassandra table.

In order to run a Cassandra sink connector, you need to prepare a yaml config file including informations that Pulsar IO
runtime needs to know. For example, how Pulsar IO can find the cassandra cluster, what is the keyspace and table that
Pulsar IO will be using for writing Pulsar messages to.

Create a file `examples/cassandra-sink.yml` and edit it to fill in following content:

```
configs:
    roots: "localhost:9042"
    keyspace: "pulsar_test_keyspace"
    columnFamily: "pulsar_test_table"
    keyname: "key"
    columnName: "col"
```

To learn more about Cassandra Connector, see [Cassandra Connector](io-cassandra.md).

### Submit a Cassandra Sink

Pulsar provides the [CLI](reference-cli-tools.md) for running and managing Pulsar I/O connectors.

We can run following command to sink a sink connector with type `cassandra` and config file `examples/cassandra-sink.yml`.

#### Note

> The `sink-type` parameter of the currently built-in connectors is determined by the setting of the `name` parameter specified in the pulsar-io.yaml file.

```shell
bin/pulsar-admin sink create \
    --tenant public \
    --namespace default \
    --name cassandra-test-sink \
    --sink-type cassandra \
    --sink-config-file examples/cassandra-sink.yml \
    --inputs test_cassandra
```

Once the command is executed, Pulsar will create a sink connector named `cassandra-test-sink` and the sink connector will be running
as a Pulsar Function and write the messages produced in topic `test_cassandra` to Cassandra table `pulsar_test_table`.

### Inspect the Cassandra Sink

You can use [sink CLI](reference-pulsar-admin.md#sink) and [source CLI](reference-pulsar-admin.md#source)
for inspecting and managing the IO connectors.

#### Retrieve Sink Info

```bash
bin/pulsar-admin sink get \
    --tenant public \
    --namespace default \
    --name cassandra-test-sink
```

Example output:

```shell
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

#### Check Sink Running Status

```bash
bin/pulsar-admin sink status \
    --tenant public \
    --namespace default \
    --name cassandra-test-sink
```

Example output:

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

### Verify the Cassandra Sink

Now lets produce some messages to the input topic of the Cassandra sink `test_cassandra`.

```bash
for i in {0..9}; do bin/pulsar-client produce -m "key-$i" -n 1 test_cassandra; done
```

Inspect the sink running status again. You should be able to see 10 messages are processed by the Cassandra sink.

```bash
bin/pulsar-admin sink status \
    --tenant public \
    --namespace default \
    --name cassandra-test-sink
```

Example output:

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
      "numReadFromPulsar" : 10,
      "numSystemExceptions" : 0,
      "latestSystemExceptions" : [ ],
      "numSinkExceptions" : 0,
      "latestSinkExceptions" : [ ],
      "numWrittenToSink" : 10,
      "lastReceivedTime" : 1551685489136,
      "workerId" : "c-standalone-fw-localhost-8080"
    }
  } ]
}
```

Finally, lets inspect the results in Cassandra using `cqlsh`

```bash
docker exec -ti cassandra cqlsh localhost
```

Select the rows from the Cassandra table `pulsar_test_table`:

```bash
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
```

### Delete the Cassandra Sink

```shell
bin/pulsar-admin sink delete \
    --tenant public \
    --namespace default \
    --name cassandra-test-sink
```
