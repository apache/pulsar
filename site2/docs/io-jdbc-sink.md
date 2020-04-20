---
id: io-jdbc-sink
title: JDBC sink connector
sidebar_label: JDBC sink connector
---

The JDBC sink connector pulls messages from Pulsar topics 
and persists the messages to MySQL or SQlite.

> Currently, INSERT, DELETE and UPDATE operations are supported.

## Build NAR package with MySQL (Optional)

Because of the license of MySQL, if you need to persist the messages to MySQL, you can manually package the JDBC NAR with
MySQL before using the JDBC io connector.

```shell
git clone https://github.com/apache/pulsar.git
git checkout v2.5.0                                   // checkout to a stable version
cd pulsar-io/jdbc
```

Add the following MySQL dependency to the `pom.xml` under the jdbc directory.

```
<dependency>
  <groupId>mysql</groupId>
  <artifactId>mysql-connector-java</artifactId>
  <version>${mysql-jdbc.version}</version>
  <scope>runtime</scope>
</dependency>
```

Then run the following command to package a JDBC NAR package.

```shell script
mvn clean install package -DskipTests
```

Then you will find the NAR package under the `target` directory.

## Configuration 

The configuration of the JDBC sink connector has the following properties.

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

### Example

Before using the JDBC sink connector, you need to create a configuration file through one of the following methods.

* JSON 

    ```json
    {
        "userName": "root",
        "password": "jdbc",
        "jdbcUrl": "jdbc:mysql://127.0.0.1:3306/pulsar_mysql_jdbc_sink",
        "tableName": "pulsar_mysql_jdbc_sink"
    }
    ```

* YAML

    ```yaml
    configs:
        userName: "root"
        password: "jdbc"
        jdbcUrl: "jdbc:mysql://127.0.0.1:3306/pulsar_mysql_jdbc_sink"
        tableName: "pulsar_mysql_jdbc_sink"
    ```

## Usage

For more information about **how to use a JDBC sink connector**, see [connect Pulsar to MySQL](io-quickstart.md#connect-pulsar-to-mysql).
