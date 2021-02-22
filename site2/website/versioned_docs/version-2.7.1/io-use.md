---
id: version-2.7.1-io-use
title: How to use Pulsar connectors
sidebar_label: Use
original_id: io-use
---

This guide describes how to use Pulsar connectors.

## Install a connector

Pulsar bundles several [builtin connectors](io-connectors.md) used to move data in and out of commonly used systems (such as database and messaging system). Optionally, you can create and use your desired non-builtin connectors.

> #### Note
> 
> When using a non-builtin connector, you need to specify the path of a archive file for the connector.

To set up a builtin connector, follow
the instructions [here](getting-started-standalone.md#installing-builtin-connectors).

After the setup, the builtin connector is automatically discovered by Pulsar brokers (or function-workers), so no additional installation steps are required.

## Configure a connector

You can configure the following information:

* [Configure a default storage location for a connector](#configure-a-default-storage-location-for-a-connector)

* [Configure a connector with a YAML file](#configure-a-connector-with-yaml-file)

### Configure a default storage location for a connector

To configure a default folder for builtin connectors, set the `connectorsDirectory` parameter in the `./conf/functions_worker.yml` configuration file.

**Example**

Set the `./connectors` folder as the default storage location for builtin connectors.

```
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

```shell
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

```shell
configs:
   bootstrapServers: "pulsar-kafka:9092"
   groupId: "test-pulsar-io"
   topic: "my-topic"
   sessionTimeoutMs: "10000"
   autoCommitEnabled: "false"
```

**Example 3**

Below is a YAML configuration file of a PostgreSQL JDBC sink.

```shell
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

If you add or delete a nar file in a connector folder, reload the available builtin connector before using it.

#### Source

Use the `reload` subcommand.

```shell
$ pulsar-admin sources reload
```

For more information, see [`here`](io-cli.md#reload).

#### Sink

Use the `reload` subcommand.

```shell
$ pulsar-admin sinks reload
```

For more information, see [`here`](io-cli.md#reload-1).

### `available`

After reloading connectors (optional), you can get a list of available connectors.

#### Source

Use the `available-sources` subcommand.

```shell
$ pulsar-admin sources available-sources
```

#### Sink

Use the `available-sinks` subcommand.

```shell
$ pulsar-admin sinks available-sinks
```

## Run a connector

To run a connector, you can perform the following operations:

* [Create a connector](#create)
  
* [Start a connector](#start)

* [Run a connector locally](#localrun)

### `create`

You can create a connector using **Admin CLI**, **REST API** or **JAVA admin API**.f

#### Source

Create a source connector.

<!--DOCUSAURUS_CODE_TABS-->

<!--Admin CLI-->

Use the `create` subcommand.

```
$ pulsar-admin sources create options
```

For more information, see [here](io-cli.md#create).

<!--REST API-->

Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sources/:tenant/:namespace/:sourceName|operation/registerSource?version=[[pulsar:version_number]]}

<!--Java Admin API-->

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

    For more information, see [`createSource`](https://pulsar.apache.org/api/admin/org/apache/pulsar/client/admin/Source.html#createSource-SourceConfig-java.lang.String-).

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
  
    For more information, see [`createSourceWithUrl`](https://pulsar.apache.org/api/admin/org/apache/pulsar/client/admin/Source.html#createSourceWithUrl-SourceConfig-java.lang.String-).

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sink

Create a sink connector.

<!--DOCUSAURUS_CODE_TABS-->

<!--Admin CLI-->

Use the `create` subcommand.

```
$ pulsar-admin sinks create options
```

For more information, see [here](io-cli.md#create-1).

<!--REST API-->

Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sinks/:tenant/:namespace/:sinkName|operation/registerSink?version=[[pulsar:version_number]]}

<!--Java Admin API-->

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

    For more information, see [`createSink`](https://pulsar.apache.org/api/admin/org/apache/pulsar/client/admin/Sink.html#createSink-SinkConfig-java.lang.String-).

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
  
    For more information, see [`createSinkWithUrl`](https://pulsar.apache.org/api/admin/org/apache/pulsar/client/admin/Sink.html#createSinkWithUrl-SinkConfig-java.lang.String-).

<!--END_DOCUSAURUS_CODE_TABS-->

### `start`

You can start a connector using **Admin CLI** or **REST API**.

#### Source

Start a source connector.

<!--DOCUSAURUS_CODE_TABS-->

<!--Admin CLI-->

Use the `start` subcommand.

```
$ pulsar-admin sources start options
```

For more information, see [here](io-cli.md#start).

<!--REST API-->

* Start **all** source connectors.

    Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sources/:tenant/:namespace/:sourceName/start|operation/startSource?version=[[pulsar:version_number]]}

* Start a **specified** source connector.

    Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sources/:tenant/:namespace/:sourceName/:instanceId/start|operation/startSource?version=[[pulsar:version_number]]}

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sink

Start a sink connector.

<!--DOCUSAURUS_CODE_TABS-->

<!--Admin CLI-->

Use the `start` subcommand.

```
$ pulsar-admin sinks start options
```

For more information, see [here](io-cli.md#start-1).

<!--REST API-->

* Start **all** sink connectors.

    Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sources/:tenant/:namespace/:sinkName/start|operation/startSink?version=[[pulsar:version_number]]}

* Start a **specified** sink connector.

    Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sinks/:tenant/:namespace/:sourceName/:instanceId/start|operation/startSink?version=[[pulsar:version_number]]}

<!--END_DOCUSAURUS_CODE_TABS-->

### `localrun`

You can run a connector locally rather than deploying it on a Pulsar cluster using **Admin CLI**.

#### Source

Run a source connector locally.

<!--DOCUSAURUS_CODE_TABS-->

<!--Admin CLI-->

Use the `localrun` subcommand.

```
$ pulsar-admin sources localrun options
```

For more information, see [here](io-cli.md#localrun).

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sink

Run a sink connector locally.

<!--DOCUSAURUS_CODE_TABS-->

<!--Admin CLI-->

Use the `localrun` subcommand.

```
$ pulsar-admin sinks localrun options
```

For more information, see [here](io-cli.md#localrun-1).

<!--END_DOCUSAURUS_CODE_TABS-->

## Monitor a connector

To monitor a connector, you can perform the following operations:

* [Get the information of a connector](#get)

* [Get the list of all running connectors](#list)

* [Get the current status of a connector](#status)

### `get`

You can get the information of a connector using **Admin CLI**, **REST API** or **JAVA admin API**.

#### Source

Get the information of a source connector.

<!--DOCUSAURUS_CODE_TABS-->

<!--Admin CLI-->

Use the `get` subcommand.

```
$ pulsar-admin sources get options
```

For more information, see [here](io-cli.md#get).

<!--REST API-->

Send a `GET` request to this endpoint: {@inject: endpoint|GET|/admin/v3/sources/:tenant/:namespace/:sourceName|operation/getSourceInfo?version=[[pulsar:version_number]]}

<!--Java Admin API-->

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

```
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

For more information, see [`getSource`](https://pulsar.apache.org/api/admin/org/apache/pulsar/client/admin/Source.html#getSource-java.lang.String-java.lang.String-java.lang.String-).

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sink

Get the information of a sink connector.

<!--DOCUSAURUS_CODE_TABS-->

<!--Admin CLI-->

Use the `get` subcommand.

```
$ pulsar-admin sinks get options
```

For more information, see [here](io-cli.md#get-1).

<!--REST API-->

Send a `GET` request to this endpoint: {@inject: endpoint|GET|/admin/v3/sinks/:tenant/:namespace/:sinkName|operation/getSinkInfo?version=[[pulsar:version_number]]}

<!--Java Admin API-->

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

For more information, see [`getSink`](https://pulsar.apache.org/api/admin/org/apache/pulsar/client/admin/Sink.html#getSink-java.lang.String-java.lang.String-java.lang.String-).

<!--END_DOCUSAURUS_CODE_TABS-->

### `list`

You can get the list of all running connectors using **Admin CLI**, **REST API** or **JAVA admin API**.

#### Source

Get the list of all running source connectors.

<!--DOCUSAURUS_CODE_TABS-->

<!--Admin CLI-->

Use the `list` subcommand.

```
$ pulsar-admin sources list options
```

For more information, see [here](io-cli.md#list).

<!--REST API-->

Send a `GET` request to this endpoint: {@inject: endpoint|GET|/admin/v3/sources/:tenant/:namespace/|operation/listSources?version=[[pulsar:version_number]]}

<!--Java Admin API-->

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

For more information, see [`listSource`](https://pulsar.apache.org/api/admin/org/apache/pulsar/client/admin/Source.html#listSources-java.lang.String-java.lang.String-).

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sink

Get the list of all running sink connectors.

<!--DOCUSAURUS_CODE_TABS-->

<!--Admin CLI-->

Use the `list` subcommand.

```
$ pulsar-admin sinks list options
```

For more information, see [here](io-cli.md#list-1).

<!--REST API-->

Send a `GET` request to this endpoint: {@inject: endpoint|GET|/admin/v3/sinks/:tenant/:namespace/|operation/listSinks?version=[[pulsar:version_number]]}

<!--Java Admin API-->

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

For more information, see [`listSource`](https://pulsar.apache.org/api/admin/org/apache/pulsar/client/admin/Sink.html#listSinks-java.lang.String-java.lang.String-).

<!--END_DOCUSAURUS_CODE_TABS-->

### `status`

You can get the current status of a connector using **Admin CLI**, **REST API** or **JAVA admin API**.

#### Source

Get the current status of a source connector.

<!--DOCUSAURUS_CODE_TABS-->

<!--Admin CLI-->

Use the `status` subcommand.

```
$ pulsar-admin sources status options
```

For more information, see [here](io-cli.md#status).

<!--REST API-->

* Get the current status of **all** source connectors.
  
  Send a `GET` request to this endpoint: {@inject: endpoint|GET|/admin/v3/sources/:tenant/:namespace/:sourceName/status|operation/getSourceStatus?version=[[pulsar:version_number]]}

* Gets the current status of a **specified** source connector.

  Send a `GET` request to this endpoint: {@inject: endpoint|GET|/admin/v3/sources/:tenant/:namespace/:sourceName/:instanceId/status|operation/getSourceStatus?version=[[pulsar:version_number]]}
  
<!--Java Admin API-->

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

    For more information, see [`getSourceStatus`](https://pulsar.apache.org/api/admin/org/apache/pulsar/client/admin/Source.html#getSource-java.lang.String-java.lang.String-java.lang.String-).

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

    For more information, see [`getSourceStatus`](https://pulsar.apache.org/api/admin/org/apache/pulsar/client/admin/Source.html#getSourceStatus-java.lang.String-java.lang.String-java.lang.String-int-).

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sink

Get the current status of a Pulsar sink connector.

<!--DOCUSAURUS_CODE_TABS-->

<!--Admin CLI-->

Use the `status` subcommand.

```
$ pulsar-admin sinks status options
```

For more information, see [here](io-cli.md#status-1).

<!--REST API-->

* Get the current status of **all** sink connectors.
  
  Send a `GET` request to this endpoint: {@inject: endpoint|GET|/admin/v3/sinks/:tenant/:namespace/:sinkName/status|operation/getSinkStatus?version=[[pulsar:version_number]]}

* Gets the current status of a **specified** sink connector.

  Send a `GET` request to this endpoint: {@inject: endpoint|GET|/admin/v3/sinks/:tenant/:namespace/:sourceName/:instanceId/status|operation/getSinkInstanceStatus?version=[[pulsar:version_number]]}

<!--Java Admin API-->

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

    For more information, see [`getSinkStatus`](https://pulsar.apache.org/api/admin/org/apache/pulsar/client/admin/Sink.html#getSinkStatus-java.lang.String-java.lang.String-java.lang.String-).

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

    For more information, see [`getSinkStatusWithInstanceID`](https://pulsar.apache.org/api/admin/org/apache/pulsar/client/admin/Sink.html#getSinkStatus-java.lang.String-java.lang.String-java.lang.String-int-).

<!--END_DOCUSAURUS_CODE_TABS-->

## Update a connector

### `update`

You can update a running connector using **Admin CLI**, **REST API** or **JAVA admin API**.

#### Source

Update a running Pulsar source connector.

<!--DOCUSAURUS_CODE_TABS-->

<!--Admin CLI-->

Use the `update` subcommand.

```
$ pulsar-admin sources update options
```

For more information, see [here](io-cli.md#update).

<!--REST API-->

Send a `PUT` request to this endpoint: {@inject: endpoint|PUT|/admin/v3/sources/:tenant/:namespace/:sourceName|operation/updateSource?version=[[pulsar:version_number]]}
  
<!--Java Admin API-->

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

    For more information, see [`updateSource`](https://pulsar.apache.org/api/admin/org/apache/pulsar/client/admin/Source.html#updateSource-SourceConfig-java.lang.String-).

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

For more information, see [`createSourceWithUrl`](https://pulsar.apache.org/api/admin/org/apache/pulsar/client/admin/Source.html#updateSourceWithUrl-SourceConfig-java.lang.String-).

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sink

Update a running Pulsar sink connector.

<!--DOCUSAURUS_CODE_TABS-->

<!--Admin CLI-->

Use the `update` subcommand.

```
$ pulsar-admin sinks update options
```

For more information, see [here](io-cli.md#update-1).

<!--REST API-->

Send a `PUT` request to this endpoint: {@inject: endpoint|PUT|/admin/v3/sinks/:tenant/:namespace/:sinkName|operation/updateSink?version=[[pulsar:version_number]]}
  
<!--Java Admin API-->

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

    For more information, see [`updateSink`](https://pulsar.apache.org/api/admin/org/apache/pulsar/client/admin/Sink.html#updateSink-SinkConfig-java.lang.String-).

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

For more information, see [`updateSinkWithUrl`](https://pulsar.apache.org/api/admin/org/apache/pulsar/client/admin/Sink.html#updateSinkWithUrl-SinkConfig-java.lang.String-).

<!--END_DOCUSAURUS_CODE_TABS-->

## Stop a connector

### `stop`

You can stop a connector using **Admin CLI**, **REST API** or **JAVA admin API**.

#### Source

Stop a source connector.

<!--DOCUSAURUS_CODE_TABS-->

<!--Admin CLI-->

Use the `stop` subcommand.

```
$ pulsar-admin sources stop options
```

For more information, see [here](io-cli.md#stop).

<!--REST API-->

* Stop **all** source connectors.
  
  Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sources/:tenant/:namespace/:sourceName|operation/stopSource?version=[[pulsar:version_number]]}

* Stop a **specified** source connector.
  
  Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sources/:tenant/:namespace/:sourceName/:instanceId|operation/stopSource?version=[[pulsar:version_number]]}

<!--Java Admin API-->

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

    For more information, see [`stopSource`](https://pulsar.apache.org/api/admin/org/apache/pulsar/client/admin/Source.html#stopSource-java.lang.String-java.lang.String-java.lang.String-).

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

    For more information, see [`stopSource`](https://pulsar.apache.org/api/admin/org/apache/pulsar/client/admin/Source.html#stopSource-java.lang.String-java.lang.String-java.lang.String-int-).

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sink

Stop a sink connector.

<!--DOCUSAURUS_CODE_TABS-->

<!--Admin CLI-->

Use the `stop` subcommand.

```
$ pulsar-admin sinks stop options
```

For more information, see [here](io-cli.md#stop-1).

<!--REST API-->

* Stop **all** sink connectors.
  
  Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sinks/:tenant/:namespace/:sinkName/stop|operation/stopSink?version=[[pulsar:version_number]]}

* Stop a **specified** sink connector.
  
  Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sources/:tenant/:namespace/:sinkeName/:instanceId/stop|operation/stopSink?version=[[pulsar:version_number]]}

<!--Java Admin API-->

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

    For more information, see [`stopSink`](https://pulsar.apache.org/api/admin/org/apache/pulsar/client/admin/Sink.html#stopSink-java.lang.String-java.lang.String-java.lang.String-).

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

    For more information, see [`stopSink`](https://pulsar.apache.org/api/admin/org/apache/pulsar/client/admin/Sink.html#stopSink-java.lang.String-java.lang.String-java.lang.String-int-).

<!--END_DOCUSAURUS_CODE_TABS-->

## Restart a connector

### `restart`

You can restart a connector using **Admin CLI**, **REST API** or **JAVA admin API**.

#### Source

Restart a source connector.

<!--DOCUSAURUS_CODE_TABS-->

<!--Admin CLI-->

Use the `restart` subcommand.

```
$ pulsar-admin sources restart options
```

For more information, see [here](io-cli.md#restart).

<!--REST API-->

* Restart **all** source connectors.
  
  Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sources/:tenant/:namespace/:sourceName/restart|operation/restartSource?version=[[pulsar:version_number]]}

* Restart a **specified** source connector.
  
  Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sources/:tenant/:namespace/:sourceName/:instanceId/restart|operation/restartSource?version=[[pulsar:version_number]]}

<!--Java Admin API-->

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

    For more information, see [`restartSource`](https://pulsar.apache.org/api/admin/org/apache/pulsar/client/admin/Source.html#restartSource-java.lang.String-java.lang.String-java.lang.String-).

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

    For more information, see [`restartSource`](https://pulsar.apache.org/api/admin/org/apache/pulsar/client/admin/Source.html#restartSource-java.lang.String-java.lang.String-java.lang.String-int-).

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sink

Restart a sink connector.

<!--DOCUSAURUS_CODE_TABS-->

<!--Admin CLI-->

Use the `restart` subcommand.

```
$ pulsar-admin sinks restart options
```

For more information, see [here](io-cli.md#restart-1).

<!--REST API-->

* Restart **all** sink connectors.
  
  Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sources/:tenant/:namespace/:sinkName/restart|operation/restartSource?version=[[pulsar:version_number]]}

* Restart a **specified** sink connector.
  
  Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sources/:tenant/:namespace/:sinkName/:instanceId/restart|operation/restartSource?version=[[pulsar:version_number]]}

<!--Java Admin API-->

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

    For more information, see [`restartSink`](https://pulsar.apache.org/api/admin/org/apache/pulsar/client/admin/Sink.html#restartSink-java.lang.String-java.lang.String-java.lang.String-).

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

    For more information, see [`restartSink`](https://pulsar.apache.org/api/admin/org/apache/pulsar/client/admin/Sink.html#restartSink-java.lang.String-java.lang.String-java.lang.String-int-).

<!--END_DOCUSAURUS_CODE_TABS-->

## Delete a connector

### `delete`

You can delete a connector using **Admin CLI**, **REST API** or **JAVA admin API**.

#### Source

Delete a source connector.

<!--DOCUSAURUS_CODE_TABS-->

<!--Admin CLI-->

Use the `delete` subcommand.

```
$ pulsar-admin sources delete options
```

For more information, see [here](io-cli.md#delete).

<!--REST API-->

Delete al Pulsar source connector.
  
Send a `DELETE` request to this endpoint: {@inject: endpoint|DELETE|/admin/v3/sources/:tenant/:namespace/:sourceName|operation/deregisterSource?version=[[pulsar:version_number]]}

<!--Java Admin API-->

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

For more information, see [`deleteSource`](https://pulsar.apache.org/api/admin/org/apache/pulsar/client/admin/Source.html#deleteSource-java.lang.String-java.lang.String-java.lang.String-).

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sink

Delete a sink connector.

<!--DOCUSAURUS_CODE_TABS-->

<!--Admin CLI-->

Use the `delete` subcommand.

```
$ pulsar-admin sinks delete options
```

For more information, see [here](io-cli.md#delete-1).

<!--REST API-->

Delete a sink connector.
  
Send a `DELETE` request to this endpoint: {@inject: endpoint|DELETE|/admin/v3/sinks/:tenant/:namespace/:sinkName|operation/deregisterSink?version=[[pulsar:version_number]]}

<!--Java Admin API-->

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

For more information, see [`deleteSource`](https://pulsar.apache.org/api/admin/org/apache/pulsar/client/admin/Sink.html#deleteSink-java.lang.String-java.lang.String-java.lang.String-).

<!--END_DOCUSAURUS_CODE_TABS-->
