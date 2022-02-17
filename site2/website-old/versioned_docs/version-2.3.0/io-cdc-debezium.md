---
id: version-2.3.0-io-cdc-debezium
title: CDC Debezium Connector
sidebar_label: CDC Debezium Connector
original_id: io-cdc-debezium
---

### Source Configuration Options

The Configuration is mostly related to Debezium task config, besides this we should provides the service URL of Pulsar cluster, and topic names that used to store offset and history.

| Name | Required | Default | Description |
|------|----------|---------|-------------|
| `task.class` | `true` | `null` | A source task class that implemented in Debezium. |
| `database.hostname` | `true` | `null` | The address of the Database server. |
| `database.port` | `true` | `null` | The port number of the Database server.. |
| `database.user` | `true` | `null` | The name of the Database user that has the required privileges. |
| `database.password` | `true` | `null` | The password for the Database user that has the required privileges. |
| `database.server.id` | `true` | `null` | The connector’s identifier that must be unique within the Database cluster and similar to Database’s server-id configuration property. |
| `database.server.name` | `true` | `null` | The logical name of the Database server/cluster, which forms a namespace and is used in all the names of the Kafka topics to which the connector writes, the Kafka Connect schema names, and the namespaces of the corresponding Avro schema when the Avro Connector is used. |
| `database.whitelist` | `false` | `null` | A list of all databases hosted by this server that this connector will monitor. This is optional, and there are other properties for listing the databases and tables to include or exclude from monitoring. |
| `key.converter` | `true` | `null` | The converter provided by Kafka Connect to convert record key. |
| `value.converter` | `true` | `null` | The converter provided by Kafka Connect to convert record value.  |
| `database.history` | `true` | `null` | The name of the database history class name. |
| `database.history.pulsar.topic` | `true` | `null` | The name of the database history topic where the connector will write and recover DDL statements. This topic is for internal use only and should not be used by consumers. |
| `database.history.pulsar.service.url` | `true` | `null` | Pulsar cluster service url for history topic. |
| `pulsar.service.url` | `true` | `null` | Pulsar cluster service URL for the offset topic used in Debezium. You can use the `bin/pulsar-admin --admin-url http://pulsar:8080 sources localrun --source-config-file configs/pg-pulsar-config.yaml` command to point to the target Pulsar cluster. |
| `offset.storage.topic` | `true` | `null` | Record the last committed offsets that the connector successfully completed. |

### Configuration Example

Here is a configuration Json example:

```$json
{
    "tenant": "public",
    "namespace": "default",
    "name": "debezium-kafka-source",
    "className": "org.apache.pulsar.io.kafka.connect.KafkaConnectSource" ,
    "topicName": "kafka-connect-topic",
    "configs":
    {
        "task.class": "io.debezium.connector.mysql.MySqlConnectorTask",
        "database.hostname": "localhost",
        "database.port": "3306",
        "database.user": "debezium",
        "database.password": "dbz",
        "database.server.id": "184054",
        "database.server.name": "dbserver1",
        "database.whitelist": "inventory",
        "database.history": "org.apache.pulsar.io.debezium.PulsarDatabaseHistory",
        "database.history.pulsar.topic": "history-topic",
        "database.history.pulsar.service.url": "pulsar://127.0.0.1:6650",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "pulsar.service.url": "pulsar://127.0.0.1:6650",
        "offset.storage.topic": "offset-topic"
    },
    "archive": "connectors/pulsar-io-kafka-connect-adaptor-2.3.0-SNAPSHOT.nar"
}
```

You could also find the yaml example in this [file](https://github.com/apache/pulsar/blob/master/pulsar-io/kafka-connect-adaptor/src/main/resources/debezium-mysql-source-config.yaml), which has similar content below:

```$yaml
tenant: "public"
namespace: "default"
name: "debezium-kafka-source"
topicName: "kafka-connect-topic"
archive: "connectors/pulsar-io-kafka-connect-adaptor-2.3.0-SNAPSHOT.nar"

##autoAck: true
parallelism: 1

configs:
  ## sourceTask
  task.class: "io.debezium.connector.mysql.MySqlConnectorTask"

  ## config for mysql, docker image: debezium/example-mysql:0.8
  database.hostname: "localhost"
  database.port: "3306"
  database.user: "debezium"
  database.password: "dbz"
  database.server.id: "184054"
  database.server.name: "dbserver1"
  database.whitelist: "inventory"

  database.history: "org.apache.pulsar.io.debezium.PulsarDatabaseHistory"
  database.history.pulsar.topic: "history-topic"
  database.history.pulsar.service.url: "pulsar://127.0.0.1:6650"
  ## KEY_CONVERTER_CLASS_CONFIG, VALUE_CONVERTER_CLASS_CONFIG
  key.converter: "org.apache.kafka.connect.json.JsonConverter"
  value.converter: "org.apache.kafka.connect.json.JsonConverter"
  ## PULSAR_SERVICE_URL_CONFIG
  pulsar.service.url: "pulsar://127.0.0.1:6650"
  ## OFFSET_STORAGE_TOPIC_CONFIG
  offset.storage.topic: "offset-topic"
```

### Usage example

Here is a simple example to store MySQL change data using above example config.

- Start a MySQL server with an example database, from which Debezium can capture changes.
```$bash
 docker run -it --rm --name mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=debezium -e MYSQL_USER=mysqluser -e MYSQL_PASSWORD=mysqlpw debezium/example-mysql:0.8
```

- Start a Pulsar service locally in standalone mode.
```$bash
 bin/pulsar standalone
```

- Start pulsar debezium connector, with local run mode, and using above yaml config file. Please make sure that the nar file is available as configured in path `connectors/pulsar-io-kafka-connect-adaptor-2.3.0-SNAPSHOT.nar`.
```$bash
 bin/pulsar-admin source localrun  --sourceConfigFile debezium-mysql-source-config.yaml
```

- Subscribe the topic for table `inventory.products`.
```
 bin/pulsar-client consume -s "sub-products" public/default/dbserver1.inventory.products -n 0
```

- start a MySQL cli docker connector, and use it we could change to the table `products` in MySQL server.
```$bash
$docker run -it --rm --name mysqlterm --link mysql --rm mysql:5.7 sh -c 'exec mysql -h"$MYSQL_PORT_3306_TCP_ADDR" -P"$MYSQL_PORT_3306_TCP_PORT" -uroot -p"$MYSQL_ENV_MYSQL_ROOT_PASSWORD"'
```

This command will pop out MySQL cli, in this cli, we could do a change in table products, use commands below to change the name of 2 items in table products:

```
mysql> use inventory;
mysql> show tables;
mysql> SELECT * FROM  products ;
mysql> UPDATE products SET name='1111111111' WHERE id=101;
mysql> UPDATE products SET name='1111111111' WHERE id=107;
```

- In above subscribe topic terminal tab, we could find that 2 changes has been kept into products topic.

## FAQ
 
### Debezium postgres connector will hang when create snap

```$xslt
#18 prio=5 os_prio=31 tid=0x00007fd83096f800 nid=0xa403 waiting on condition [0x000070000f534000]
    java.lang.Thread.State: WAITING (parking)
     at sun.misc.Unsafe.park(Native Method)
     - parking to wait for  <0x00000007ab025a58> (a java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject)
     at java.util.concurrent.locks.LockSupport.park(LockSupport.java:175)
     at java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.await(AbstractQueuedSynchronizer.java:2039)
     at java.util.concurrent.LinkedBlockingDeque.putLast(LinkedBlockingDeque.java:396)
     at java.util.concurrent.LinkedBlockingDeque.put(LinkedBlockingDeque.java:649)
     at io.debezium.connector.base.ChangeEventQueue.enqueue(ChangeEventQueue.java:132)
     at io.debezium.connector.postgresql.PostgresConnectorTask$$Lambda$203/385424085.accept(Unknown Source)
     at io.debezium.connector.postgresql.RecordsSnapshotProducer.sendCurrentRecord(RecordsSnapshotProducer.java:402)
     at io.debezium.connector.postgresql.RecordsSnapshotProducer.readTable(RecordsSnapshotProducer.java:321)
     at io.debezium.connector.postgresql.RecordsSnapshotProducer.lambda$takeSnapshot$6(RecordsSnapshotProducer.java:226)
     at io.debezium.connector.postgresql.RecordsSnapshotProducer$$Lambda$240/1347039967.accept(Unknown Source)
     at io.debezium.jdbc.JdbcConnection.queryWithBlockingConsumer(JdbcConnection.java:535)
     at io.debezium.connector.postgresql.RecordsSnapshotProducer.takeSnapshot(RecordsSnapshotProducer.java:224)
     at io.debezium.connector.postgresql.RecordsSnapshotProducer.lambda$start$0(RecordsSnapshotProducer.java:87)
     at io.debezium.connector.postgresql.RecordsSnapshotProducer$$Lambda$206/589332928.run(Unknown Source)
     at java.util.concurrent.CompletableFuture.uniRun(CompletableFuture.java:705)
     at java.util.concurrent.CompletableFuture.uniRunStage(CompletableFuture.java:717)
     at java.util.concurrent.CompletableFuture.thenRun(CompletableFuture.java:2010)
     at io.debezium.connector.postgresql.RecordsSnapshotProducer.start(RecordsSnapshotProducer.java:87)
     at io.debezium.connector.postgresql.PostgresConnectorTask.start(PostgresConnectorTask.java:126)
     at io.debezium.connector.common.BaseSourceTask.start(BaseSourceTask.java:47)
     at org.apache.pulsar.io.kafka.connect.KafkaConnectSource.open(KafkaConnectSource.java:127)
     at org.apache.pulsar.io.debezium.DebeziumSource.open(DebeziumSource.java:100)
     at org.apache.pulsar.functions.instance.JavaInstanceRunnable.setupInput(JavaInstanceRunnable.java:690)
     at org.apache.pulsar.functions.instance.JavaInstanceRunnable.setupJavaInstance(JavaInstanceRunnable.java:200)
     at org.apache.pulsar.functions.instance.JavaInstanceRunnable.run(JavaInstanceRunnable.java:230)
     at java.lang.Thread.run(Thread.java:748)
``` 

If you encounter the above problems in synchronizing data, please refer to [this](https://github.com/apache/pulsar/issues/4075) and add the following configuration to the configuration file:

```$xslt
max.queue.size=
```