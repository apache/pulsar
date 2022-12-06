---
id: io-use
title: How to use Pulsar connectors
sidebar_label: "Use"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````


This guide describes how to use Pulsar connectors.

## Install a connector

Pulsar bundles several [built-in connectors](io-connectors.md) used to move data in and out of commonly used systems (such as database and messaging system). Optionally, you can create and use your desired non-built-in connectors.

:::note

When using a non-built-in connector, you need to specify the path of an archive file for the connector.

:::

To set up a built-in connector, follow the [instructions](io-quickstart.md#install-pulsar-and-built-in-connector).

After the setup, the built-in connector is automatically discovered by Pulsar brokers (or function-workers), so no additional installation steps are required.

## Configure a connector

You can configure the following information:

* [Configure a default storage location for a connector](#configure-a-default-storage-location-for-a-connector)

* [Configure a connector with a YAML file](#configure-a-connector-with-a-yaml-file)

### Configure a default storage location for a connector

To configure a default folder for built-in connectors, set the `connectorsDirectory` parameter in the `./conf/functions_worker.yml` configuration file.

**Example**

Set the `./connectors` folder as the default storage location for built-in connectors.

```shell
########################
# Connectors
########################

connectorsDirectory: ./connectors
```

### Configure a connector with a YAML file

To configure a connector, you need to provide a YAML configuration file when creating a connector.

The YAML configuration file tells Pulsar where to locate connectors and how to connect connectors with Pulsar topics.

**Example 1**

Below is a YAML configuration file of a Cassandra sink, which tells Pulsar:

* Which Cassandra cluster to connect

* What is the `keyspace` and `columnFamily` to be used in Cassandra for collecting data

* How to map Pulsar messages into Cassandra table key and columns

```yaml
tenant: public
namespace: default
name: cassandra-test-sink
...
# cassandra specific config
configs:
    roots: "localhost:9042"
    keyspace: "pulsar_test_keyspace"
    columnFamily: "pulsar_test_table"
    keyname: "key"
    columnName: "col"
```

**Example 2**

Below is a YAML configuration file of a Kafka source.

```yaml
configs:
   bootstrapServers: "pulsar-kafka:9092"
   groupId: "test-pulsar-io"
   topic: "my-topic"
   sessionTimeoutMs: "10000"
   autoCommitEnabled: "false"
```

**Example 3**

Below is a YAML configuration file of a PostgreSQL JDBC sink.

```yaml
configs:
   userName: "postgres"
   password: "password"
   jdbcUrl: "jdbc:postgresql://localhost:5432/test_jdbc"
   tableName: "test_jdbc"
```

## Get available connectors

Before starting using connectors, you can perform the following operations:

* [Reload connectors](#reload)

* [Get a list of available connectors](#get-available-connectors)

### `reload`

If you add or delete a nar file in a connector folder, reload the available built-in connector before using it.

#### Source

Use the `reload` subcommand.

```shell
pulsar-admin sources reload
```

For the latest and complete information, see [Pulsar admin docs](/tools/pulsar-admin/).

#### Sink

Use the `reload` subcommand.

```shell
pulsar-admin sinks reload
```

For the latest and complete information, see [Pulsar admin docs](/tools/pulsar-admin/).

### `available`

After reloading connectors (optional), you can get a list of available connectors.

#### Source

Use the `available-sources` subcommand.

```shell
pulsar-admin sources available-sources
```

#### Sink

Use the `available-sinks` subcommand.

```shell
pulsar-admin sinks available-sinks
```

## Run a connector

To run a connector, you can perform the following operations:

* [Create a connector](#create)

* [Start a connector](#start)

* [Run a connector locally](#localrun)

### `create`

You can create a connector using **Admin CLI**, **REST API** or **JAVA admin API**.

#### Source

Create a source connector.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the `create` subcommand.

```shell
pulsar-admin sources create options
```

For the latest and complete information, see [Pulsar admin docs](/tools/pulsar-admin/).

</TabItem>
<TabItem value="REST API">

Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sources/:tenant/:namespace/:sourceName|operation/registerSource?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java Admin API">

* Create a source connector with a **local file**.

  ```java
  void createSource(SourceConfig sourceConfig,
                    String fileName)
             throws PulsarAdminException
  ```

  **Parameter**

  |Name|Description
  |---|---
  `sourceConfig` | The source configuration object

   **Exception**

  |Name|Description|
  |---|---
  | `PulsarAdminException` | Unexpected error

  For more information, see [`createSource`](/api/admin/org/apache/pulsar/client/admin/Source.html#createSource-SourceConfig-java.lang.String-).

* Create a source connector using a **remote file** with a URL from which fun-pkg can be downloaded.

  ```java
  void createSourceWithUrl(SourceConfig sourceConfig,
                           String pkgUrl)
                    throws PulsarAdminException
  ```

  Supported URLs are `http` and `file`.

  **Example**

  * HTTP: http://www.repo.com/fileName.jar

  * File: file:///dir/fileName.jar

  **Parameter**

  Parameter| Description
  |---|---
  `sourceConfig` | The source configuration object
  `pkgUrl` | URL from which pkg can be downloaded

  **Exception**

  |Name|Description|
  |---|---
  | `PulsarAdminException` | Unexpected error

  For more information, see [`createSourceWithUrl`](/api/admin/org/apache/pulsar/client/admin/Source.html#createSourceWithUrl-SourceConfig-java.lang.String-).

</TabItem>

</Tabs>
````

#### Sink

Create a sink connector.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the `create` subcommand.

```shell
pulsar-admin sinks create options
```

For the latest and complete information, see [Pulsar admin docs](/tools/pulsar-admin/).

</TabItem>
<TabItem value="REST API">

Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sinks/:tenant/:namespace/:sinkName|operation/registerSink?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java Admin API">

* Create a sink connector with a **local file**.

  ```java
  void createSink(SinkConfig sinkConfig,
                  String fileName)
           throws PulsarAdminException
  ```

  **Parameter**

  |Name|Description
  |---|---
  `sinkConfig` | The sink configuration object

  **Exception**

  |Name|Description|
  |---|---
  | `PulsarAdminException` | Unexpected error

  For more information, see [`createSink`](/api/admin/org/apache/pulsar/client/admin/Sink.html#createSink-SinkConfig-java.lang.String-).

* Create a sink connector using a **remote file** with a URL from which fun-pkg can be downloaded.

  ```java
  void createSinkWithUrl(SinkConfig sinkConfig,
                      String pkgUrl)
                  throws PulsarAdminException
  ```

  Supported URLs are `http` and `file`.

  **Example**

  * HTTP: http://www.repo.com/fileName.jar

  * File: file:///dir/fileName.jar

  **Parameter**

  Parameter| Description
  |---|---
  `sinkConfig` | The sink configuration object
  `pkgUrl` | URL from which pkg can be downloaded

  **Exception**

  |Name|Description|
  |---|---
  | `PulsarAdminException` | Unexpected error

  For more information, see [`createSinkWithUrl`](/api/admin/org/apache/pulsar/client/admin/Sink.html#createSinkWithUrl-SinkConfig-java.lang.String-).

</TabItem>

</Tabs>
````

### `start`

You can start a connector using **Admin CLI** or **REST API**.

#### Source

Start a source connector.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"}]}>

<TabItem value="Admin CLI">

Use the `start` subcommand.

```shell
pulsar-admin sources start options
```

For the latest and complete information, see [Pulsar admin docs](/tools/pulsar-admin/).

</TabItem>
<TabItem value="REST API">

* Start **all** source connectors.

  Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sources/:tenant/:namespace/:sourceName/start|operation/startSource?version=@pulsar:version_number@}

* Start a **specified** source connector.

  Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sources/:tenant/:namespace/:sourceName/:instanceId/start|operation/startSource?version=@pulsar:version_number@}

</TabItem>

</Tabs>
````

#### Sink

Start a sink connector.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"}]}>

<TabItem value="Admin CLI">

Use the `start` subcommand.

```shell
pulsar-admin sinks start options
```

For the latest and complete information, see [Pulsar admin docs](/tools/pulsar-admin/).

</TabItem>
<TabItem value="REST API">

* Start **all** sink connectors.

  Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sources/:tenant/:namespace/:sinkName/start|operation/startSink?version=@pulsar:version_number@}

* Start a **specified** sink connector.

  Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sinks/:tenant/:namespace/:sourceName/:instanceId/start|operation/startSink?version=@pulsar:version_number@}

</TabItem>

</Tabs>
````

### `localrun`

You can run a connector locally rather than deploying it on a Pulsar cluster using **Admin CLI**.

#### Source

Run a source connector locally.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"}]}>

<TabItem value="Admin CLI">

Use the `localrun` subcommand.

```shell
pulsar-admin sources localrun options
```

For the latest and complete information, see [Pulsar admin docs](/tools/pulsar-admin/).

</TabItem>

</Tabs>
````

#### Sink

Run a sink connector locally.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"}]}>

<TabItem value="Admin CLI">

Use the `localrun` subcommand.

```shell
pulsar-admin sinks localrun options
```

For the latest and complete information, see [Pulsar admin docs](/tools/pulsar-admin/).
</TabItem>

</Tabs>
````

## Monitor a connector

To monitor a connector, you can perform the following operations:

* [Get the information of a connector](#get)

* [Get the list of all running connectors](#list)

* [Get the current status of a connector](#status)

### `get`

You can get the information of a connector using **Admin CLI**, **REST API** or **JAVA admin API**.

#### Source

Get the information of a source connector.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the `get` subcommand.

```shell
pulsar-admin sources get options
```

For the latest and complete information, see [Pulsar admin docs](/tools/pulsar-admin/).

</TabItem>
<TabItem value="REST API">

Send a `GET` request to this endpoint: {@inject: endpoint|GET|/admin/v3/sources/:tenant/:namespace/:sourceName|operation/getSourceInfo?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java Admin API">

```java
SourceConfig getSource(String tenant,
                       String namespace,
                       String source)
                throws PulsarAdminException
```

**Example**

This is a sourceConfig.

```java
{
 "tenant": "tenantName",
 "namespace": "namespaceName",
 "name": "sourceName",
 "className": "className",
 "topicName": "topicName",
 "configs": {},
 "parallelism": 1,
 "processingGuarantees": "ATLEAST_ONCE",
 "resources": {
   "cpu": 1.0,
   "ram": 1073741824,
   "disk": 10737418240
 }
}
```

This is a sourceConfig example.

```json
{
 "tenant": "public",
 "namespace": "default",
 "name": "debezium-mysql-source",
 "className": "org.apache.pulsar.io.debezium.mysql.DebeziumMysqlSource",
 "topicName": "debezium-mysql-topic",
 "configs": {
   "database.user": "debezium",
   "database.server.id": "184054",
   "database.server.name": "dbserver1",
   "database.port": "3306",
   "database.hostname": "localhost",
   "database.password": "dbz",
   "database.history.pulsar.service.url": "pulsar://127.0.0.1:6650",
   "value.converter": "org.apache.kafka.connect.json.JsonConverter",
   "database.whitelist": "inventory",
   "key.converter": "org.apache.kafka.connect.json.JsonConverter",
   "database.history": "org.apache.pulsar.io.debezium.PulsarDatabaseHistory",
   "pulsar.service.url": "pulsar://127.0.0.1:6650",
   "database.history.pulsar.topic": "history-topic2"
 },
 "parallelism": 1,
 "processingGuarantees": "ATLEAST_ONCE",
 "resources": {
   "cpu": 1.0,
   "ram": 1073741824,
   "disk": 10737418240
 }
}
```

**Exception**

Exception name | Description
|---|---
`PulsarAdminException.NotAuthorizedException` | You don't have the admin permission
`PulsarAdminException.NotFoundException` | Cluster doesn't exist
`PulsarAdminException` | Unexpected error

For more information, see [`getSource`](/api/admin/org/apache/pulsar/client/admin/Source.html#getSource-java.lang.String-java.lang.String-java.lang.String-).

</TabItem>

</Tabs>
````

#### Sink

Get the information of a sink connector.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the `get` subcommand.

```shell
pulsar-admin sinks get options
```

For the latest and complete information, see [Pulsar admin docs](/tools/pulsar-admin/).

</TabItem>
<TabItem value="REST API">

Send a `GET` request to this endpoint: {@inject: endpoint|GET|/admin/v3/sinks/:tenant/:namespace/:sinkName|operation/getSinkInfo?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java Admin API">

```java
SinkConfig getSink(String tenant,
                   String namespace,
                   String sink)
            throws PulsarAdminException
```

**Example**

This is a sinkConfig.

```json
{
"tenant": "tenantName",
"namespace": "namespaceName",
"name": "sinkName",
"className": "className",
"inputSpecs": {
"topicName": {
    "isRegexPattern": false
}
},
"configs": {},
"parallelism": 1,
"processingGuarantees": "ATLEAST_ONCE",
"retainOrdering": false,
"autoAck": true
}
```

This is a sinkConfig example.

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

**Parameter description**

Name| Description
|---|---
`tenant` | Tenant name
`namespace` | Namespace name
`sink` | Sink name

For more information, see [`getSink`](/api/admin/org/apache/pulsar/client/admin/Sink.html#getSink-java.lang.String-java.lang.String-java.lang.String-).

</TabItem>

</Tabs>
````

### `list`

You can get the list of all running connectors using **Admin CLI**, **REST API** or **JAVA admin API**.

#### Source

Get the list of all running source connectors.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the `list` subcommand.

```shell
pulsar-admin sources list options
```

For the latest and complete information, see [Pulsar admin docs](/tools/pulsar-admin/).

</TabItem>
<TabItem value="REST API">

Send a `GET` request to this endpoint: {@inject: endpoint|GET|/admin/v3/sources/:tenant/:namespace|operation/listSources?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java Admin API">

```java
List<String> listSources(String tenant,
                         String namespace)
                  throws PulsarAdminException
```

**Response example**

```java
["f1", "f2", "f3"]
```

**Exception**

Exception name | Description
|---|---
`PulsarAdminException.NotAuthorizedException` | You don't have the admin permission
`PulsarAdminException` | Unexpected error

For more information, see [`listSource`](/api/admin/org/apache/pulsar/client/admin/Source.html#listSources-java.lang.String-java.lang.String-).

</TabItem>

</Tabs>
````

#### Sink

Get the list of all running sink connectors.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the `list` subcommand.

```shell
pulsar-admin sinks list options
```

For the latest and complete information, see [Pulsar admin docs](/tools/pulsar-admin/).

</TabItem>
<TabItem value="REST API">

Send a `GET` request to this endpoint: {@inject: endpoint|GET|/admin/v3/sinks/:tenant/:namespace|operation/listSinks?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java Admin API">

```java
List<String> listSinks(String tenant,
                       String namespace)
                throws PulsarAdminException
```

**Response example**

```java
["f1", "f2", "f3"]
```

**Exception**

Exception name | Description
|---|---
`PulsarAdminException.NotAuthorizedException` | You don't have the admin permission
`PulsarAdminException` | Unexpected error

For more information, see [`listSource`](/api/admin/org/apache/pulsar/client/admin/Sink.html#listSinks-java.lang.String-java.lang.String-).

</TabItem>

</Tabs>
````

### `status`

You can get the current status of a connector using **Admin CLI**, **REST API** or **JAVA admin API**.

#### Source

Get the current status of a source connector.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the `status` subcommand.

```shell
pulsar-admin sources status options
```

For the latest and complete information, see [Pulsar admin docs](/tools/pulsar-admin/).

</TabItem>
<TabItem value="REST API">

* Get the current status of **all** source connectors.

  Send a `GET` request to this endpoint: {@inject: endpoint|GET|/admin/v3/sources/:tenant/:namespace/:sourceName/status|operation/getSourceStatus?version=@pulsar:version_number@}

* Gets the current status of a **specified** source connector.

  Send a `GET` request to this endpoint: {@inject: endpoint|GET|/admin/v3/sources/:tenant/:namespace/:sourceName/:instanceId/status|operation/getSourceStatus?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java Admin API">

* Get the current status of **all** source connectors.

  ```java
  SourceStatus getSourceStatus(String tenant,
                              String namespace,
                              String source)
                      throws PulsarAdminException
  ```

  **Parameter**

  Parameter| Description
  |---|---
  `tenant` | Tenant name
  `namespace` | Namespace name
  `sink` | Source name

  **Exception**

  Name | Description
  |---|---
  `PulsarAdminException` | Unexpected error

  For more information, see [`getSourceStatus`](/api/admin/org/apache/pulsar/client/admin/Source.html#getSource-java.lang.String-java.lang.String-java.lang.String-).

* Gets the current status of a **specified** source connector.

  ```java
  SourceStatus.SourceInstanceStatus.SourceInstanceStatusData getSourceStatus(String tenant,
                                                                             String namespace,
                                                                             String source,
                                                                             int id)
                                                                      throws PulsarAdminException
  ```

  **Parameter**

  Parameter| Description
  |---|---
  `tenant` | Tenant name
  `namespace` | Namespace name
  `sink` | Source name
  `id` | Source instanceID

  **Exception**

  Exception name | Description
  |---|---
  `PulsarAdminException` | Unexpected error

  For more information, see [`getSourceStatus`](/api/admin/org/apache/pulsar/client/admin/Source.html#getSourceStatus-java.lang.String-java.lang.String-java.lang.String-int-).

</TabItem>

</Tabs>
````

#### Sink

Get the current status of a Pulsar sink connector.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the `status` subcommand.

```shell
pulsar-admin sinks status options
```

For the latest and complete information, see [Pulsar admin docs](/tools/pulsar-admin/).

</TabItem>
<TabItem value="REST API">

* Get the current status of **all** sink connectors.

  Send a `GET` request to this endpoint: {@inject: endpoint|GET|/admin/v3/sinks/:tenant/:namespace/:sinkName/status|operation/getSinkStatus?version=@pulsar:version_number@}

* Gets the current status of a **specified** sink connector.

  Send a `GET` request to this endpoint: {@inject: endpoint|GET|/admin/v3/sinks/:tenant/:namespace/:sourceName/:instanceId/status|operation/getSinkInstanceStatus?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java Admin API">

* Get the current status of **all** sink connectors.

  ```java
  SinkStatus getSinkStatus(String tenant,
                           String namespace,
                           String sink)
                    throws PulsarAdminException
  ```

  **Parameter**

  Parameter| Description
  |---|---
  `tenant` | Tenant name
  `namespace` | Namespace name
  `sink` | Source name

  **Exception**

  Exception name | Description
  |---|---
  `PulsarAdminException` | Unexpected error

  For more information, see [`getSinkStatus`](/api/admin/org/apache/pulsar/client/admin/Sink.html#getSinkStatus-java.lang.String-java.lang.String-java.lang.String-).

* Gets the current status of a **specified** source connector.

  ```java
  SinkStatus.SinkInstanceStatus.SinkInstanceStatusData getSinkStatus(String tenant,
                                                                     String namespace,
                                                                     String sink,
                                                                     int id)
                                                              throws PulsarAdminException
  ```

  **Parameter**

  Parameter| Description
  |---|---
  `tenant` | Tenant name
  `namespace` | Namespace name
  `sink` | Source name
  `id` | Sink instanceID

  **Exception**

  Exception name | Description
  |---|---
  `PulsarAdminException` | Unexpected error

  For more information, see [`getSinkStatusWithInstanceID`](/api/admin/org/apache/pulsar/client/admin/Sink.html#getSinkStatus-java.lang.String-java.lang.String-java.lang.String-int-).

</TabItem>

</Tabs>
````

## Update a connector

### `update`

You can update a running connector using **Admin CLI**, **REST API** or **JAVA admin API**.

#### Source

Update a running Pulsar source connector.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the `update` subcommand.

```shell
pulsar-admin sources update options
```

For the latest and complete information, see [Pulsar admin docs](/tools/pulsar-admin/).

</TabItem>
<TabItem value="REST API">

Send a `PUT` request to this endpoint: {@inject: endpoint|PUT|/admin/v3/sources/:tenant/:namespace/:sourceName|operation/updateSource?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java Admin API">

* Update a running source connector with a **local file**.

  ```java
  void updateSource(SourceConfig sourceConfig,
                  String fileName)
          throws PulsarAdminException
  ```

  **Parameter**

  | Name | Description
  |---|---
  |`sourceConfig` | The source configuration object

  **Exception**

  |Name|Description|
  |---|---
  |`PulsarAdminException.NotAuthorizedException`| You don't have the admin permission
  | `PulsarAdminException.NotFoundException` | Cluster doesn't exist
  | `PulsarAdminException` | Unexpected error

  For more information, see [`updateSource`](/api/admin/org/apache/pulsar/client/admin/Source.html#updateSource-SourceConfig-java.lang.String-).

* Update a source connector using a **remote file** with a URL from which fun-pkg can be downloaded.

  ```java
  void updateSourceWithUrl(SourceConfig sourceConfig,
                       String pkgUrl)
                throws PulsarAdminException
  ```

  Supported URLs are `http` and `file`.

  **Example**

  * HTTP: http://www.repo.com/fileName.jar

  * File: file:///dir/fileName.jar

  **Parameter**

  | Name | Description
  |---|---
  | `sourceConfig` | The source configuration object
  | `pkgUrl` | URL from which pkg can be downloaded

  **Exception**

  |Name|Description|
  |---|---
  |`PulsarAdminException.NotAuthorizedException`| You don't have the admin permission
  | `PulsarAdminException.NotFoundException` | Cluster doesn't exist
  | `PulsarAdminException` | Unexpected error

For more information, see [`createSourceWithUrl`](/api/admin/org/apache/pulsar/client/admin/Source.html#updateSourceWithUrl-SourceConfig-java.lang.String-).

</TabItem>

</Tabs>
````

#### Sink

Update a running Pulsar sink connector.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the `update` subcommand.

```shell
pulsar-admin sinks update options
```

For the latest and complete information, see [Pulsar admin docs](/tools/pulsar-admin/).

</TabItem>
<TabItem value="REST API">

Send a `PUT` request to this endpoint: {@inject: endpoint|PUT|/admin/v3/sinks/:tenant/:namespace/:sinkName|operation/updateSink?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java Admin API">

* Update a running sink connector with a **local file**.

  ```java
  void updateSink(SinkConfig sinkConfig,
                  String fileName)
       throws PulsarAdminException
  ```

  **Parameter**

  | Name | Description
  |---|---
  |`sinkConfig` | The sink configuration object

  **Exception**

  |Name|Description|
  |---|---
  |`PulsarAdminException.NotAuthorizedException`| You don't have the admin permission
  | `PulsarAdminException.NotFoundException` | Cluster doesn't exist
  | `PulsarAdminException` | Unexpected error

  For more information, see [`updateSink`](/api/admin/org/apache/pulsar/client/admin/Sink.html#updateSink-SinkConfig-java.lang.String-).

* Update a sink connector using a **remote file** with a URL from which fun-pkg can be downloaded.

  ```java
  void updateSinkWithUrl(SinkConfig sinkConfig,
                         String pkgUrl)
                  throws PulsarAdminException
  ```

  Supported URLs are `http` and `file`.

  **Example**

  * HTTP: http://www.repo.com/fileName.jar

  * File: file:///dir/fileName.jar

  **Parameter**

  | Name | Description
  |---|---
  | `sinkConfig` | The sink configuration object
  | `pkgUrl` | URL from which pkg can be downloaded

  **Exception**

  |Name|Description|
  |---|---
  |`PulsarAdminException.NotAuthorizedException`| You don't have the admin permission
  |`PulsarAdminException.NotFoundException` | Cluster doesn't exist
  |`PulsarAdminException` | Unexpected error

For more information, see [`updateSinkWithUrl`](/api/admin/org/apache/pulsar/client/admin/Sink.html#updateSinkWithUrl-SinkConfig-java.lang.String-).

</TabItem>

</Tabs>
````

## Stop a connector

### `stop`

You can stop a connector using **Admin CLI**, **REST API** or **JAVA admin API**.

#### Source

Stop a source connector.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the `stop` subcommand.

```shell
pulsar-admin sources stop options
```

For the latest and complete information, see [Pulsar admin docs](/tools/pulsar-admin/).

</TabItem>
<TabItem value="REST API">

* Stop **all** source connectors.

  Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sources/:tenant/:namespace/:sourceName|operation/stopSource?version=@pulsar:version_number@}

* Stop a **specified** source connector.

  Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sources/:tenant/:namespace/:sourceName/:instanceId|operation/stopSource?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java Admin API">

* Stop **all** source connectors.

  ```java
  void stopSource(String tenant,
                  String namespace,
                  String source)
          throws PulsarAdminException
  ```

  **Parameter**

  | Name | Description
  |---|---
  `tenant` | Tenant name
  `namespace` | Namespace name
  `source` | Source name

  **Exception**

  |Name|Description|
  |---|---
  | `PulsarAdminException` | Unexpected error

  For more information, see [`stopSource`](/api/admin/org/apache/pulsar/client/admin/Source.html#stopSource-java.lang.String-java.lang.String-java.lang.String-).

* Stop a **specified** source connector.

  ```java
  void stopSource(String tenant,
                  String namespace,
                  String source,
                  int instanceId)
           throws PulsarAdminException
  ```

  **Parameter**

  | Name | Description
  |---|---
  `tenant` | Tenant name
  `namespace` | Namespace name
  `source` | Source name
   `instanceId` | Source instanceID

  **Exception**

  |Name|Description|
  |---|---
  | `PulsarAdminException` | Unexpected error

  For more information, see [`stopSource`](/api/admin/org/apache/pulsar/client/admin/Source.html#stopSource-java.lang.String-java.lang.String-java.lang.String-int-).

</TabItem>

</Tabs>
````

#### Sink

Stop a sink connector.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the `stop` subcommand.

```
pulsar-admin sinks stop options
```

For the latest and complete information, see [Pulsar admin docs](/tools/pulsar-admin/).

</TabItem>
<TabItem value="REST API">

* Stop **all** sink connectors.

  Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sinks/:tenant/:namespace/:sinkName/stop|operation/stopSink?version=@pulsar:version_number@}

* Stop a **specified** sink connector.

  Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sources/:tenant/:namespace/:sinkeName/:instanceId/stop|operation/stopSink?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java Admin API">

* Stop **all** sink connectors.

  ```java
  void stopSink(String tenant,
              String namespace,
              String sink)
      throws PulsarAdminException
  ```

  **Parameter**

  | Name | Description
  |---|---
  `tenant` | Tenant name
  `namespace` | Namespace name
  `source` | Source name

  **Exception**

  |Name|Description|
  |---|---
  | `PulsarAdminException` | Unexpected error

  For more information, see [`stopSink`](/api/admin/org/apache/pulsar/client/admin/Sink.html#stopSink-java.lang.String-java.lang.String-java.lang.String-).

* Stop a **specified** sink connector.

  ```java
  void stopSink(String tenant,
                String namespace,
                String sink,
                int instanceId)
         throws PulsarAdminException
  ```

  **Parameter**

  | Name | Description
  |---|---
  `tenant` | Tenant name
  `namespace` | Namespace name
  `source` | Source name
  `instanceId` | Source instanceID

  **Exception**

  |Name|Description|
  |---|---
  | `PulsarAdminException` | Unexpected error

  For more information, see [`stopSink`](/api/admin/org/apache/pulsar/client/admin/Sink.html#stopSink-java.lang.String-java.lang.String-java.lang.String-int-).

</TabItem>

</Tabs>
````

## Restart a connector

### `restart`

You can restart a connector using **Admin CLI**, **REST API** or **JAVA admin API**.

#### Source

Restart a source connector.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the `restart` subcommand.

```shell
pulsar-admin sources restart options
```

For the latest and complete information, see [Pulsar admin docs](/tools/pulsar-admin/).

</TabItem>
<TabItem value="REST API">

* Restart **all** source connectors.

  Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sources/:tenant/:namespace/:sourceName/restart|operation/restartSource?version=@pulsar:version_number@}

* Restart a **specified** source connector.

  Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sources/:tenant/:namespace/:sourceName/:instanceId/restart|operation/restartSource?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java Admin API">

* Restart **all** source connectors.

  ```java
  void restartSource(String tenant,
                     String namespace,
                     String source)
              throws PulsarAdminException
  ```

  **Parameter**

  | Name | Description
  |---|---
  `tenant` | Tenant name
  `namespace` | Namespace name
  `source` | Source name

  **Exception**

  |Name|Description|
  |---|---
  | `PulsarAdminException` | Unexpected error

  For more information, see [`restartSource`](/api/admin/org/apache/pulsar/client/admin/Source.html#restartSource-java.lang.String-java.lang.String-java.lang.String-).

* Restart a **specified** source connector.

  ```java
  void restartSource(String tenant,
                     String namespace,
                     String source,
                     int instanceId)
              throws PulsarAdminException
  ```

  **Parameter**

  | Name | Description
  |---|---
  `tenant` | Tenant name
  `namespace` | Namespace name
  `source` | Source name
   `instanceId` | Source instanceID

  **Exception**

  |Name|Description|
  |---|---
  | `PulsarAdminException` | Unexpected error

  For more information, see [`restartSource`](/api/admin/org/apache/pulsar/client/admin/Source.html#restartSource-java.lang.String-java.lang.String-java.lang.String-int-).

</TabItem>

</Tabs>
````

#### Sink

Restart a sink connector.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the `restart` subcommand.

```shell
pulsar-admin sinks restart options
```

For the latest and complete information, see [Pulsar admin docs](/tools/pulsar-admin/).

</TabItem>
<TabItem value="REST API">

* Restart **all** sink connectors.

  Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sources/:tenant/:namespace/:sinkName/restart|operation/restartSource?version=@pulsar:version_number@}

* Restart a **specified** sink connector.

  Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sources/:tenant/:namespace/:sinkName/:instanceId/restart|operation/restartSource?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java Admin API">

* Restart all Pulsar sink connectors.

  ```java
  void restartSink(String tenant,
                   String namespace,
                   String sink)
            throws PulsarAdminException
  ```

  **Parameter**

  | Name | Description
  |---|---
  `tenant` | Tenant name
  `namespace` | Namespace name
  `sink` | Sink name

  **Exception**

  |Name|Description|
  |---|---
  | `PulsarAdminException` | Unexpected error

  For more information, see [`restartSink`](/api/admin/org/apache/pulsar/client/admin/Sink.html#restartSink-java.lang.String-java.lang.String-java.lang.String-).

* Restart a **specified** sink connector.

  ```java
  void restartSink(String tenant,
                   String namespace,
                   String sink,
                   int instanceId)
            throws PulsarAdminException
  ```

  **Parameter**

  | Name | Description
  |---|---
  `tenant` | Tenant name
  `namespace` | Namespace name
  `source` | Source name
   `instanceId` | Sink instanceID

  **Exception**

  |Name|Description|
  |---|---
  | `PulsarAdminException` | Unexpected error

  For more information, see [`restartSink`](/api/admin/org/apache/pulsar/client/admin/Sink.html#restartSink-java.lang.String-java.lang.String-java.lang.String-int-).

</TabItem>

</Tabs>
````

## Delete a connector

### `delete`

You can delete a connector using **Admin CLI**, **REST API** or **JAVA admin API**.

#### Source

Delete a source connector.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the `delete` subcommand.

```shell
pulsar-admin sources delete options
```

For the latest and complete information, see [Pulsar admin docs](/tools/pulsar-admin/).

</TabItem>
<TabItem value="REST API">

Delete al Pulsar source connector.

Send a `DELETE` request to this endpoint: {@inject: endpoint|DELETE|/admin/v3/sources/:tenant/:namespace/:sourceName|operation/deregisterSource?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java Admin API">

Delete a source connector.

```java
void deleteSource(String tenant,
                  String namespace,
                  String source)
           throws PulsarAdminException
```

**Parameter**

| Name | Description
|---|---
`tenant` | Tenant name
`namespace` | Namespace name
`source` | Source name

**Exception**

|Name|Description|
|---|---
|`PulsarAdminException.NotAuthorizedException`| You don't have the admin permission
| `PulsarAdminException.NotFoundException` | Cluster doesn't exist
| `PulsarAdminException.PreconditionFailedException` | Cluster is not empty
| `PulsarAdminException` | Unexpected error

For more information, see [`deleteSource`](/api/admin/org/apache/pulsar/client/admin/Source.html#deleteSource-java.lang.String-java.lang.String-java.lang.String-).

</TabItem>

</Tabs>
````

#### Sink

Delete a sink connector.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Admin CLI"
  values={[{"label":"Admin CLI","value":"Admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java Admin API","value":"Java Admin API"}]}>

<TabItem value="Admin CLI">

Use the `delete` subcommand.

```shell
pulsar-admin sinks delete options
```

For the latest and complete information, see [Pulsar admin docs](/tools/pulsar-admin/).

</TabItem>
<TabItem value="REST API">

Delete a sink connector.

Send a `DELETE` request to this endpoint: {@inject: endpoint|DELETE|/admin/v3/sinks/:tenant/:namespace/:sinkName|operation/deregisterSink?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java Admin API">

Delete a Pulsar sink connector.

```java
void deleteSink(String tenant,
                String namespace,
                String source)
         throws PulsarAdminException
```

**Parameter**

| Name | Description
|---|---
`tenant` | Tenant name
`namespace` | Namespace name
`sink` | Sink name

**Exception**

|Name|Description|
|---|---
|`PulsarAdminException.NotAuthorizedException`| You don't have the admin permission
| `PulsarAdminException.NotFoundException` | Cluster doesn't exist
| `PulsarAdminException.PreconditionFailedException` | Cluster is not empty
| `PulsarAdminException` | Unexpected error

For more information, see [`deleteSource`](/api/admin/org/apache/pulsar/client/admin/Sink.html#deleteSink-java.lang.String-java.lang.String-java.lang.String-).

</TabItem>

</Tabs>
````
