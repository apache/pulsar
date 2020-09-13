---
id: version-2.6.2-io-quickstart
title: How to connect Pulsar to database
sidebar_label: Get started
original_id: io-quickstart
---

This tutorial provides a hands-on look at how you can move data out of Pulsar without writing a single line of code.  

It is helpful to review the [concepts](io-overview.md) for Pulsar I/O with running the steps in this guide to gain a deeper understanding.   

At the end of this tutorial, you are able to:

- [Connect Pulsar to Cassandra](#Connect-Pulsar-to-Cassandra)
  
- [Connect Pulsar to PostgreSQL](#Connect-Pulsar-to-PostgreSQL)

> #### Tip
>
> * These instructions assume you are running Pulsar in [standalone mode](getting-started-standalone.md). However, all
> the commands used in this tutorial can be used in a multi-nodes Pulsar cluster without any changes.
>
> * All the instructions are assumed to run at the root directory of a Pulsar binary distribution.

## Install Pulsar and built-in connector

Before connecting Pulsar to a database, you need to install Pulsar and the desired built-in connector.

For more information about **how to install a standalone Pulsar and built-in connectors**, see [here](getting-started-standalone.md/#installing-pulsar).

## Start Pulsar standalone 

1. Start Pulsar locally.

    ```bash
    bin/pulsar standalone
    ```

    All the components of a Pulsar service are start in order. 
    
    You can curl those pulsar service endpoints to make sure Pulsar service is up running correctly.

2. Check Pulsar binary protocol port.

    ```bash
    telnet localhost 6650
    ```

3. Check Pulsar Function cluster.

    ```bash
    curl -s http://localhost:8080/admin/v2/worker/cluster
    ```

    **Example output**
    ```json
    [{"workerId":"c-standalone-fw-localhost-6750","workerHostname":"localhost","port":6750}]
    ```

4. Make sure a public tenant and a default namespace exist.

    ```bash
    curl -s http://localhost:8080/admin/v2/namespaces/public
    ```

    **Example output**
    ```json
    ["public/default","public/functions"]
    ```

5. All built-in connectors should be listed as available.

    ```bash
    curl -s http://localhost:8080/admin/v2/functions/connectors
    ```

    **Example output**

    ```json
    [{"name":"aerospike","description":"Aerospike database sink","sinkClass":"org.apache.pulsar.io.aerospike.AerospikeStringSink"},{"name":"cassandra","description":"Writes data into Cassandra","sinkClass":"org.apache.pulsar.io.cassandra.CassandraStringSink"},{"name":"kafka","description":"Kafka source and sink connector","sourceClass":"org.apache.pulsar.io.kafka.KafkaStringSource","sinkClass":"org.apache.pulsar.io.kafka.KafkaBytesSink"},{"name":"kinesis","description":"Kinesis sink connector","sinkClass":"org.apache.pulsar.io.kinesis.KinesisSink"},{"name":"rabbitmq","description":"RabbitMQ source connector","sourceClass":"org.apache.pulsar.io.rabbitmq.RabbitMQSource"},{"name":"twitter","description":"Ingest data from Twitter firehose","sourceClass":"org.apache.pulsar.io.twitter.TwitterFireHose"}]
    ```

    If an error occurs when starting Pulsar service, you may see an exception at the terminal running `pulsar/standalone`,
    or you can navigate to the `logs` directory under the Pulsar directory to view the logs.

## Connect Pulsar to Cassandra

This section demonstrates how to connect Pulsar to Cassandra.

> #### Tip
> 
> * Make sure you have Docker installed. If you do not have one, see [install Docker](https://docs.docker.com/docker-for-mac/install/).
> 
> * The Cassandra sink connector reads messages from Pulsar topics and writes the messages into Cassandra tables. For more information, see [Cassandra sink connector](io-cassandra-sink.md).

### Setup a Cassandra cluster

This example uses `cassandra` Docker image to start a single-node Cassandra cluster in Docker.

1. Start a Cassandra cluster.

    ```bash
    docker run -d --rm --name=cassandra -p 9042:9042 cassandra
    ```

    > **Note**
    > 
    > Before moving to the next steps, make sure the Cassandra cluster is running.

2. Make sure the Docker process is running.

    ```bash
    docker ps
    ```

3. Check the Cassandra logs to make sure the Cassandra process is running as expected.

    ```bash
    docker logs cassandra
    ```

4. Check the status of the Cassandra cluster.

    ```bash
    docker exec cassandra nodetool status
    ```

    **Example output**

    ```
    Datacenter: datacenter1
    =======================
    Status=Up/Down
    |/ State=Normal/Leaving/Joining/Moving
    --  Address     Load       Tokens       Owns (effective)  Host ID                               Rack
    UN  172.17.0.2  103.67 KiB  256          100.0%            af0e4b2f-84e0-4f0b-bb14-bd5f9070ff26  rack1
    ```

5. Use `cqlsh` to connect to the Cassandra cluster. 

    ```bash
    $ docker exec -ti cassandra cqlsh localhost
    Connected to Test Cluster at localhost:9042.
    [cqlsh 5.0.1 | Cassandra 3.11.2 | CQL spec 3.4.4 | Native protocol v4]
    Use HELP for help.
    cqlsh>
    ```

6. Create a keyspace `pulsar_test_keyspace`.

    ```bash
    cqlsh> CREATE KEYSPACE pulsar_test_keyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};
    ```

7. Create a table `pulsar_test_table`.

    ```bash
    cqlsh> USE pulsar_test_keyspace;
    cqlsh:pulsar_test_keyspace> CREATE TABLE pulsar_test_table (key text PRIMARY KEY, col text);
    ```

### Configure a Cassandra sink

Now that we have a Cassandra cluster running locally. 

In this section, you need to configure a Cassandra sink connector.

To run a Cassandra sink connector, you need to prepare a configuration file including the information that Pulsar connector runtime needs to know. 

For example, how Pulsar connector can find the Cassandra cluster, what is the keyspace and the table that Pulsar connector uses for writing Pulsar messages to, and so on.

You can create a configuration file through one of the following methods.

* JSON

    ```json
    {
        "roots": "localhost:9042",
        "keyspace": "pulsar_test_keyspace",
        "columnFamily": "pulsar_test_table",
        "keyname": "key",
        "columnName": "col"
    }
    ```

* YAML
  
    ```yaml
    configs:
        roots: "localhost:9042"
        keyspace: "pulsar_test_keyspace"
        columnFamily: "pulsar_test_table"
        keyname: "key"
        columnName: "col"
    ```
  
For more information, see [Cassandra sink connector](io-cassandra-sink.md).

### Create a Cassandra sink

You can use the [Connector Admin CLI](io-cli.md) 
to create a sink connector and perform other operations on them.

Run the following command to create a Cassandra sink connector with sink type _cassandra_ and the config file _examples/cassandra-sink.yml_ created previously.

```bash
bin/pulsar-admin sinks create \
    --tenant public \
    --namespace default \
    --name cassandra-test-sink \
    --sink-type cassandra \
    --sink-config-file examples/cassandra-sink.yml \
    --inputs test_cassandra
```

Once the command is executed, Pulsar creates the sink connector _cassandra-test-sink_. 

This sink connector runs
as a Pulsar Function and writes the messages produced in the topic _test_cassandra_ to the Cassandra table _pulsar_test_table_.

### Inspect a Cassandra sink

You can use the [Connector Admin CLI](io-cli.md) 
to monitor a connector and perform other operations on it.

* Get the information of a Cassandra sink. 

  ```bash
  bin/pulsar-admin sinks get \
      --tenant public \
      --namespace default \
      --name cassandra-test-sink
  ```

  **Example output**

  ```json
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

* Check the status of a Cassandra sink. 

  ```bash
  bin/pulsar-admin sinks status \
      --tenant public \
      --namespace default \
      --name cassandra-test-sink
  ```

  **Example output**

  ```json
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

### Verify a Cassandra sink

1. Produce some messages to the input topic of the Cassandra sink _test_cassandra_.

    ```bash
    for i in {0..9}; do bin/pulsar-client produce -m "key-$i" -n 1 test_cassandra; done
    ```

2. Inspect the status of the Cassandra sink _test_cassandra_.

    ```bash
    bin/pulsar-admin sinks status \
        --tenant public \
        --namespace default \
        --name cassandra-test-sink
    ```

    You can see 10 messages are processed by the Cassandra sink _test_cassandra_.

    **Example output**

    ```json
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

3. Use `cqlsh` to connect to the Cassandra cluster.

   ```bash
   docker exec -ti cassandra cqlsh localhost
   ```

4. Check the data of the Cassandra table _pulsar_test_table_.

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

### Delete a Cassandra Sink

You can use the [Connector Admin CLI](io-cli.md) 
to delete a connector and perform other operations on it.

```bash
bin/pulsar-admin sinks delete \
    --tenant public \
    --namespace default \
    --name cassandra-test-sink
```

## Connect Pulsar to PostgreSQL

This section demonstrates how to connect Pulsar to PostgreSQL.

> #### Tip
> 
> * Make sure you have Docker installed. If you do not have one, see [install Docker](https://docs.docker.com/docker-for-mac/install/).
> 
> * The JDBC sink connector pulls messages from Pulsar topics 
and persists the messages to ClickHouse, MariaDB, PostgreSQL, or SQlite. 
>For more information, see [JDBC sink connector](io-jdbc-sink.md).


### Setup a PostgreSQL cluster

This example uses the PostgreSQL 12 docker image to start a single-node PostgreSQL cluster in Docker.

1. Pull the PostgreSQL 12 image from Docker.

    ```bash
    $ docker pull postgres:12
    ```

2. Start PostgreSQL.

    ```bash
    $ docker run -d -it --rm \
    --name pulsar-postgres \
    -p 5432:5432 \
    -e POSTGRES_PASSWORD=password \
    -e POSTGRES_USER=postgres \    
    postgres:12
    ```

    #### Tip
    
     Flag | Description | This example
     ---|---|---|
     `-d` | To start a container in detached mode. | /
     `-it` | Keep STDIN open even if not attached and allocate a terminal. | /
     `--rm` | Remove the container automatically when it exits. | /
     `-name` | Assign a name to the container. | This example specifies _pulsar-postgres_ for the container.
     `-p` | Publish the port of the container to the host. | This example publishes the port _5432_ of the container to the host.
     `-e` | Set environment variables. | This example sets the following variables:<br>- The password for the user is _password_.<br>- The name for the user is _postgres_.

     > #### Tip
     >
     > For more information about Docker commands, see [Docker CLI](https://docs.docker.com/engine/reference/commandline/run/).

3. Check if PostgreSQL has been started successfully.

    ```bash
    $ docker logs -f pulsar-postgres
    ```

    PostgreSQL has been started successfully if the following message appears.

    ```text
    2020-05-11 20:09:24.492 UTC [1] LOG:  starting PostgreSQL 12.2 (Debian 12.2-2.pgdg100+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 8.3.0-6) 8.3.0, 64-bit
    2020-05-11 20:09:24.492 UTC [1] LOG:  listening on IPv4 address "0.0.0.0", port 5432
    2020-05-11 20:09:24.492 UTC [1] LOG:  listening on IPv6 address "::", port 5432
    2020-05-11 20:09:24.499 UTC [1] LOG:  listening on Unix socket "/var/run/postgresql/.s.PGSQL.5432"
    2020-05-11 20:09:24.523 UTC [55] LOG:  database system was shut down at 2020-05-11 20:09:24 UTC
    2020-05-11 20:09:24.533 UTC [1] LOG:  database system is ready to accept connections
    ```

4. Access to PostgreSQL.

    ```bash
    $ docker exec -it pulsar-postgres /bin/bash     
    ```

5. Create a PostgreSQL table _pulsar_postgres_jdbc_sink_.

    ```bash
    $ psql -U postgres postgres
    
    postgres=# create table if not exists pulsar_postgres_jdbc_sink
    (
    id serial PRIMARY KEY,
    name VARCHAR(255) NOT NULL    
    );
    ```

### Configure a JDBC sink

Now we have a PostgreSQL running locally. 

In this section, you need to configure a JDBC sink connector.

1. Add a configuration file.   
   
    To run a JDBC sink connector, you need to prepare a YAML configuration file including the information that Pulsar connector runtime needs to know. 
    
    For example, how Pulsar connector can find the PostgreSQL cluster, what is the JDBC URL and the table that Pulsar connector uses for writing messages to.

    Create a _pulsar-postgres-jdbc-sink.yaml_ file, copy the following contents to this file, and place the file in the `pulsar/connectors` folder.

    ```yaml
    configs:
      userName: "postgres"
      password: "password"
      jdbcUrl: "jdbc:postgresql://localhost:5432/pulsar_postgres_jdbc_sink"
      tableName: "pulsar_postgres_jdbc_sink"
    ```

2. Create a schema.

    Create a _avro-schema_ file, copy the following contents to this file, and place the file in the `pulsar/connectors` folder.

    ```json
    {
      "type": "AVRO",
      "schema": "{\"type\":\"record\",\"name\":\"Test\",\"fields\":[{\"name\":\"id\",\"type\":[\"null\",\"int\"]},{\"name\":\"name\",\"type\":[\"null\",\"string\"]}]}",
      "properties": {}
    }
    ```

    > #### Tip
    >
    > For more information about AVRO, see [Apache Avro](https://avro.apache.org/docs/1.9.1/).


3. Upload a schema to a topic.  

    This example uploads the _avro-schema_ schema to the _pulsar-postgres-jdbc-sink-topic_ topic.

    ```bash
    $ bin/pulsar-admin schemas upload pulsar-postgres-jdbc-sink-topic -f ./connectors/avro-schema
    ```

4. Check if the schema has been uploaded successfully.

    ```bash
    $ bin/pulsar-admin schemas get pulsar-postgres-jdbc-sink-topic
    ```

    The schema has been uploaded successfully if the following message appears.

    ```json
    {"name":"pulsar-postgres-jdbc-sink-topic","schema":"{\"type\":\"record\",\"name\":\"Test\",\"fields\":[{\"name\":\"id\",\"type\":[\"null\",\"int\"]},{\"name\":\"name\",\"type\":[\"null\",\"string\"]}]}","type":"AVRO","properties":{}}
    ```

### Create a JDBC sink

You can use the [Connector Admin CLI](io-cli.md) 
to create a sink connector and perform other operations on it.

This example creates a sink connector and specifies the desired information.

```bash
$ bin/pulsar-admin sinks create \
--archive ./connectors/pulsar-io-jdbc-postgres-{{pulsar:version}}.nar \
--inputs pulsar-postgres-jdbc-sink-topic \
--name pulsar-postgres-jdbc-sink \
--sink-config-file ./connectors/pulsar-postgres-jdbc-sink.yaml \
--parallelism 1
```

Once the command is executed, Pulsar creates a sink connector _pulsar-postgres-jdbc-sink_.

This sink connector runs as a Pulsar Function and writes the messages produced in the topic _pulsar-postgres-jdbc-sink-topic_ to the PostgreSQL table _pulsar_postgres_jdbc_sink_.

 #### Tip

 Flag | Description | This example 
 ---|---|---|
 `--archive` | The path to the archive file for the sink. | _pulsar-io-jdbc-postgres-{{pulsar:version}}.nar_ |
 `--inputs` | The input topic(s) of the sink. <br><br> Multiple topics can be specified as a comma-separated list.||
 `--name` | The name of the sink. | _pulsar-postgres-jdbc-sink_ |
 `--sink-config-file` | The path to a YAML config file specifying the configuration of the sink. | _pulsar-postgres-jdbc-sink.yaml_ |
 `--parallelism` | The parallelism factor of the sink. <br><br> For example, the number of sink instances to run. |  _1_ |

 > #### Tip
 >
 > For more information about `pulsar-admin sinks create options`, see [here](io-cli.md#sinks).

The sink has been created successfully if the following message appears.

```bash
"Created successfully"
```

### Inspect a JDBC sink

You can use the [Connector Admin CLI](io-cli.md) 
to monitor a connector and perform other operations on it.

* List all running JDBC sink(s).

  ```bash
  $ bin/pulsar-admin sinks list \
  --tenant public \
  --namespace default
  ```

  > #### Tip
  > 
  > For more information about `pulsar-admin sinks list options`, see [here](io-cli.md/#list-1).

  The result shows that only the _postgres-jdbc-sink_ sink is running.

  ```json
  [
  "pulsar-postgres-jdbc-sink"
  ]
  ```

* Get the information of a JDBC sink.

  ```bash
  $ bin/pulsar-admin sinks get \
  --tenant public \
  --namespace default \
  --name pulsar-postgres-jdbc-sink
  ```

  > #### Tip
  > 
  > For more information about `pulsar-admin sinks get options`, see [here](io-cli.md/#get-1).

  The result shows the information of the sink connector, including tenant, namespace, topic and so on.

  ```json
  {
    "tenant": "public",
    "namespace": "default",
    "name": "pulsar-postgres-jdbc-sink",
    "className": "org.apache.pulsar.io.jdbc.PostgresJdbcAutoSchemaSink",
    "inputSpecs": {
      "pulsar-postgres-jdbc-sink-topic": {
        "isRegexPattern": false
      }
    },
    "configs": {
      "password": "password",
      "jdbcUrl": "jdbc:postgresql://localhost:5432/pulsar_postgres_jdbc_sink",
      "userName": "postgres",
      "tableName": "pulsar_postgres_jdbc_sink"
    },
    "parallelism": 1,
    "processingGuarantees": "ATLEAST_ONCE",
    "retainOrdering": false,
    "autoAck": true
  }
  ```

* Get the status of a JDBC sink

  ```bash
  $ bin/pulsar-admin sinks status \
  --tenant public \
  --namespace default \
  --name pulsar-postgres-jdbc-sink
  ```

  > #### Tip
  > 
  > For more information about `pulsar-admin sinks status options`, see [here](io-cli.md/#status-1).

  The result shows the current status of sink connector, including the number of instance, running status, worker ID and so on.

  ```json
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
        "workerId" : "c-standalone-fw-192.168.2.52-8080"
      }
    } ]
  }
  ```

### Stop a JDBC sink

You can use the [Connector Admin CLI](io-cli.md) 
to stop a connector and perform other operations on it.

```bash
$ bin/pulsar-admin sinks stop \
--tenant public \
--namespace default \
--name pulsar-postgres-jdbc-sink
```

> #### Tip
> 
> For more information about `pulsar-admin sinks stop options`, see [here](io-cli.md/#stop-1).

The sink instance has been stopped successfully if the following message disappears.

```bash
"Stopped successfully"
```

### Restart a JDBC sink

You can use the [Connector Admin CLI](io-cli.md) 
to restart a connector and perform other operations on it.

```bash
$ bin/pulsar-admin sinks restart \
--tenant public \
--namespace default \
--name pulsar-postgres-jdbc-sink 
```

> #### Tip
> 
> For more information about `pulsar-admin sinks restart options`, see [here](io-cli.md/#restart-1).

The sink instance has been started successfully if the following message disappears.

```bash
"Started successfully"
```

> #### Tip
>
> * Optionally, you can run a standalone sink connector using `pulsar-admin sinks localrun options`. 
> 
>   Note that `pulsar-admin sinks localrun options` **runs a sink connector locally**, while `pulsar-admin sinks start options` **starts a sink connector in a cluster**.
>
> * For more information about `pulsar-admin sinks localrun options`, see [here](io-cli.md#localrun-1).

### Update a JDBC sink

You can use the [Connector Admin CLI](io-cli.md) 
to update a connector and perform other operations on it.

This example updates the parallelism of the _pulsar-postgres-jdbc-sink_ sink connector to 2.

```bash
$ bin/pulsar-admin sinks update \
--name pulsar-postgres-jdbc-sink \
--parallelism 2
```

> #### Tip
> 
> For more information about `pulsar-admin sinks update options`, see [here](io-cli.md/#update-1).

The sink connector has been updated successfully if the following message disappears.

```bash
"Updated successfully"
```

This example double-checks the information.

```bash
$ bin/pulsar-admin sinks get \
--tenant public \
--namespace default \
--name pulsar-postgres-jdbc-sink
```

The result shows that the parallelism is 2.

```json
{
  "tenant": "public",
  "namespace": "default",
  "name": "pulsar-postgres-jdbc-sink",
  "className": "org.apache.pulsar.io.jdbc.PostgresJdbcAutoSchemaSink",
  "inputSpecs": {
    "pulsar-postgres-jdbc-sink-topic": {
      "isRegexPattern": false
    }
  },
  "configs": {
    "password": "password",
    "jdbcUrl": "jdbc:postgresql://localhost:5432/pulsar_postgres_jdbc_sink",
    "userName": "postgres",
    "tableName": "pulsar_postgres_jdbc_sink"
  },
  "parallelism": 2,
  "processingGuarantees": "ATLEAST_ONCE",
  "retainOrdering": false,
  "autoAck": true
}
```

### Delete a JDBC sink

You can use the [Connector Admin CLI](io-cli.md) 
to delete a connector and perform other operations on it.

This example deletes the _pulsar-postgres-jdbc-sink_ sink connector.

```bash
$ bin/pulsar-admin sinks delete \
--tenant public \
--namespace default \
--name pulsar-postgres-jdbc-sink
```

> #### Tip
> 
> For more information about `pulsar-admin sinks delete options`, see [here](io-cli.md/#delete-1).

The sink connector has been deleted successfully if the following message appears.

```text
"Deleted successfully"
```

This example double-checks the status of the sink connector.

```bash
$ bin/pulsar-admin sinks get \
--tenant public \
--namespace default \
--name pulsar-postgres-jdbc-sink
```

The result shows that the sink connector does not exist.

```text
HTTP 404 Not Found

Reason: Sink pulsar-postgres-jdbc-sink doesn't exist
```
