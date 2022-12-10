---
id: io-jdbc-sink
title: JDBC sink connector
sidebar_label: "JDBC sink connector"
---

The JDBC sink connectors allow pulling messages from Pulsar topics and persist the messages to ClickHouse, MariaDB, PostgreSQL, and SQLite.

> Currently, INSERT, DELETE and UPDATE operations are supported.
> SQLite, MariaDB and PostgreSQL also support UPSERT operations and idempotent writes.

## Configuration 

The configuration of all JDBC sink connectors has the following properties.

### Property

| Name        | Type   | Required | Default            | Description                                                                                                                                                                                                                                                                                                                             |
|-------------|--------|----------|--------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `userName`  | String | false    | " " (empty string) | The username used to connect to the database specified by `jdbcUrl`.<br /><br />**Note: `userName` is case-sensitive.**                                                                                                                                                                                                                 |
| `password`  | String | false    | " " (empty string) | The password used to connect to the database specified by `jdbcUrl`. <br /><br />**Note: `password` is case-sensitive.**                                                                                                                                                                                                                |
| `jdbcUrl`   | String | true     | " " (empty string) | The JDBC URL of the database that the connector connects to.                                                                                                                                                                                                                                                                            |
| `tableName` | String | true     | " " (empty string) | The name of the table that the connector writes to.                                                                                                                                                                                                                                                                                     |
| `nonKey`    | String | false    | " " (empty string) | A comma-separated list containing the fields used in updating events.                                                                                                                                                                                                                                                                   |
| `key`       | String | false    | " " (empty string) | A comma-separated list containing the fields used in `where` condition of updating and deleting events.                                                                                                                                                                                                                                 |
| `timeoutMs` | int    | false    | 500                | The JDBC operation timeout in milliseconds.                                                                                                                                                                                                                                                                                             |
| `batchSize` | int    | false    | 200                | The batch size of updates made to the database.                                                                                                                                                                                                                                                                                         |
| `insertMode` | enum( INSERT,UPSERT,UPDATE) | false    | INSERT             | If it is configured as UPSERT, the sink uses upsert semantics rather than plain INSERT/UPDATE statements. Upsert semantics refer to atomically adding a new row or updating the existing row if there is a primary key constraint violation, which provides idempotence.                                                                |
| `nullValueAction` | enum(FAIL, DELETE) | false    | FAIL               | How to handle records with NULL values. Possible options are `DELETE` or `FAIL`.                                                                                                                                                                                                                                                        |
| `useTransactions` | boolean | false    | true               | Enable transactions of the database.                                                                                                                                                                                                                                                                                                    
| `excludeNonDeclaredFields` | boolean | false    | false              | All the table fields are discovered automatically. `excludeNonDeclaredFields` indicates if the table fields not explicitly listed in `nonKey` and `key` must be included in the query. By default all the table fields are included. To leverage of table fields defaults during insertion, it is suggested to set this value to `false`. |
| `useJdbcBatch`    | boolean | false    | false              | Use the JDBC batch API. This option is suggested to improve write performance. |

### Example of ClickHouse

* JSON 

  ```json
  {
     "configs": {
        "userName": "clickhouse",
        "password": "password",
        "jdbcUrl": "jdbc:clickhouse://localhost:8123/pulsar_clickhouse_jdbc_sink",
        "tableName": "pulsar_clickhouse_jdbc_sink"
        "useTransactions": "false"
     }
  }
  ```

* YAML

  ```yaml
  tenant: "public"
  namespace: "default"
  name: "jdbc-clickhouse-sink"
  inputs: [ "persistent://public/default/jdbc-clickhouse-topic" ]
  sinkType: "jdbc-clickhouse"    
  configs:
      userName: "clickhouse"
      password: "password"
      jdbcUrl: "jdbc:clickhouse://localhost:8123/pulsar_clickhouse_jdbc_sink"
      tableName: "pulsar_clickhouse_jdbc_sink"
      useTransactions: "false"
  ```

### Example of MariaDB

* JSON 

  ```json
  {
     "configs": {
        "userName": "mariadb",
        "password": "password",
        "jdbcUrl": "jdbc:mariadb://localhost:3306/pulsar_mariadb_jdbc_sink",
        "tableName": "pulsar_mariadb_jdbc_sink"
     }
  }
  ```

* YAML

  ```yaml
  tenant: "public"
  namespace: "default"
  name: "jdbc-mariadb-sink"
  inputs: [ "persistent://public/default/jdbc-mariadb-topic" ]
  sinkType: "jdbc-mariadb"    
  configs:
      userName: "mariadb"
      password: "password"
      jdbcUrl: "jdbc:mariadb://localhost:3306/pulsar_mariadb_jdbc_sink"
      tableName: "pulsar_mariadb_jdbc_sink"
  ```

### Example of OpenMLDB
> OpenMLDB does not support DELETE and UPDATE operations
* JSON

  ```json
  {
     "configs": {
        "jdbcUrl": "jdbc:openmldb:///pulsar_openmldb_db?zk=localhost:6181&zkPath=/openmldb",
        "tableName": "pulsar_openmldb_jdbc_sink"
     }
  }
  ```

* YAML

  ```yaml
  tenant: "public"
  namespace: "default"
  name: "jdbc-openmldb-sink"
  inputs: [ "persistent://public/default/jdbc-openmldb-topic" ]
  sinkType: "jdbc-openmldb"    
  configs:
      jdbcUrl: "jdbc:openmldb:///pulsar_openmldb_db?zk=localhost:6181&zkPath=/openmldb"
      tableName: "pulsar_openmldb_jdbc_sink"
  ```

### Example of PostgreSQL

Before using the JDBC PostgreSQL sink connector, you need to create a configuration file through one of the following methods.

* JSON 

  ```json
  {
     "configs": {
        "userName": "postgres",
        "password": "password",
        "jdbcUrl": "jdbc:postgresql://localhost:5432/pulsar_postgres_jdbc_sink",
        "tableName": "pulsar_postgres_jdbc_sink"
     }
  }
  ```

* YAML

  ```yaml
  tenant: "public"
  namespace: "default"
  name: "jdbc-postgres-sink"
  inputs: [ "persistent://public/default/jdbc-postgres-topic" ]
  sinkType: "jdbc-postgres"    
  configs:
      userName: "postgres"
      password: "password"
      jdbcUrl: "jdbc:postgresql://localhost:5432/pulsar_postgres_jdbc_sink"
      tableName: "pulsar_postgres_jdbc_sink"
  ```

For more information on **how to use this JDBC sink connector**, see [connect Pulsar to PostgreSQL](io-quickstart.md#connect-pulsar-to-postgresql).

### Example of SQLite

* JSON 

  ```json
  {
     "configs": {
        "jdbcUrl": "jdbc:sqlite:db.sqlite",
        "tableName": "pulsar_sqlite_jdbc_sink"
     }
  }
  ```

* YAML

  ```yaml
  tenant: "public"
  namespace: "default"
  name: "jdbc-sqlite-sink"
  inputs: [ "persistent://public/default/jdbc-sqlite-topic" ]
  sinkType: "jdbc-sqlite"    
  configs:
      jdbcUrl: "jdbc:sqlite:db.sqlite"
      tableName: "pulsar_sqlite_jdbc_sink"
  ```

