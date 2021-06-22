---
id: version-2.8.1-io-jdbc-sink
title: JDBC sink connector
sidebar_label: JDBC sink connector
original_id: io-jdbc-sink
---

The JDBC sink connectors allow pulling messages from Pulsar topics 
and persists the messages to ClickHouse, MariaDB, PostgreSQL, and SQLite.

> Currently, INSERT, DELETE and UPDATE operations are supported.

## Configuration 

The configuration of all JDBC sink connectors has the following properties.

### Property

| Name | Type|Required | Default | Description 
|------|----------|----------|---------|-------------|
| `userName` | String|false | " " (empty string) | The username used to connect to the database specified by `jdbcUrl`.<br><br>**Note: `userName` is case-sensitive.**|
| `password` | String|false | " " (empty string)| The password used to connect to the database specified by `jdbcUrl`. <br><br>**Note: `password` is case-sensitive.**|
| `jdbcUrl` | String|true | " " (empty string) | The JDBC URL of the database to which the connector connects. |
| `tableName` | String|true | " " (empty string) | The name of the table to which the connector writes. |
| `nonKey` | String|false | " " (empty string) | A comma-separated list contains the fields used in updating events.  |
| `key` | String|false | " " (empty string) | A comma-separated list contains the fields used in `where` condition of updating and deleting events. |
| `timeoutMs` | int| false|500 | The JDBC operation timeout in milliseconds. |
| `batchSize` | int|false | 200 | The batch size of updates made to the database. |

### Example for ClickHouse

* JSON 

    ```json
    {
        "userName": "clickhouse",
        "password": "password",
        "jdbcUrl": "jdbc:clickhouse://localhost:8123/pulsar_clickhouse_jdbc_sink",
        "tableName": "pulsar_clickhouse_jdbc_sink"
    }
    ```

* YAML

    ```yaml
    tenant: "public"
    namespace: "default"
    name: "jdbc-clickhouse-sink"
    topicName: "persistent://public/default/jdbc-clickhouse-topic"
    sinkType: "jdbc-clickhouse"    
    configs:
        userName: "clickhouse"
        password: "password"
        jdbcUrl: "jdbc:clickhouse://localhost:8123/pulsar_clickhouse_jdbc_sink"
        tableName: "pulsar_clickhouse_jdbc_sink"
    ```

### Example for MariaDB

* JSON 

    ```json
    {
        "userName": "mariadb",
        "password": "password",
        "jdbcUrl": "jdbc:mariadb://localhost:3306/pulsar_mariadb_jdbc_sink",
        "tableName": "pulsar_mariadb_jdbc_sink"
    }
    ```

* YAML

    ```yaml
    tenant: "public"
    namespace: "default"
    name: "jdbc-mariadb-sink"
    topicName: "persistent://public/default/jdbc-mariadb-topic"
    sinkType: "jdbc-mariadb"    
    configs:
        userName: "mariadb"
        password: "password"
        jdbcUrl: "jdbc:mariadb://localhost:3306/pulsar_mariadb_jdbc_sink"
        tableName: "pulsar_mariadb_jdbc_sink"
    ```

### Example for PostgreSQL

Before using the JDBC PostgreSQL sink connector, you need to create a configuration file through one of the following methods.

* JSON 

    ```json
    {
        "userName": "postgres",
        "password": "password",
        "jdbcUrl": "jdbc:postgresql://localhost:5432/pulsar_postgres_jdbc_sink",
        "tableName": "pulsar_postgres_jdbc_sink"
    }
    ```

* YAML

    ```yaml
    tenant: "public"
    namespace: "default"
    name: "jdbc-postgres-sink"
    topicName: "persistent://public/default/jdbc-postgres-topic"
    sinkType: "jdbc-postgres"    
    configs:
        userName: "postgres"
        password: "password"
        jdbcUrl: "jdbc:postgresql://localhost:5432/pulsar_postgres_jdbc_sink"
        tableName: "pulsar_postgres_jdbc_sink"
    ```

For more information on **how to use this JDBC sink connector**, see [connect Pulsar to PostgreSQL](io-quickstart.md#connect-pulsar-to-postgresql).

### Example for SQLite

* JSON 

    ```json
    {
        "jdbcUrl": "jdbc:sqlite:db.sqlite",
        "tableName": "pulsar_sqlite_jdbc_sink"
    }
    ```

* YAML

    ```yaml
    tenant: "public"
    namespace: "default"
    name: "jdbc-sqlite-sink"
    topicName: "persistent://public/default/jdbc-sqlite-topic"
    sinkType: "jdbc-sqlite"    
    configs:
        jdbcUrl: "jdbc:sqlite:db.sqlite"
        tableName: "pulsar_sqlite_jdbc_sink"
    ```
