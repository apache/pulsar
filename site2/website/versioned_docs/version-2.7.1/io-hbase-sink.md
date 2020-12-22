---
id: version-2.7.0-io-hbase-sink
title: HBase sink connector
sidebar_label: HBase sink connector
original_id: io-hbase-sink
---

The HBase sink connector pulls the messages from Pulsar topics 
and persists the messages to HBase tables

## Configuration

The configuration of the HBase sink connector has the following properties.

### Property

| Name | Type|Default | Required | Description |
|------|---------|----------|-------------|---
| `hbaseConfigResources` | String|None | false | HBase system configuration `hbase-site.xml` file. |
| `zookeeperQuorum` | String|None | true | HBase system configuration about `hbase.zookeeper.quorum` value. |
| `zookeeperClientPort` | String|2181 | false | HBase system configuration about `hbase.zookeeper.property.clientPort` value. |
| `zookeeperZnodeParent` | String|/hbase | false | HBase system configuration about `zookeeper.znode.parent` value. |
| `tableName` | None |String | true | HBase table, the value is `namespace:tableName`. |
| `rowKeyName` | String|None | true | HBase table rowkey name. |
| `familyName` | String|None | true | HBase table column family name. |
| `qualifierNames` |String| None | true | HBase table column qualifier names. |
| `batchTimeMs` | Long|1000l| false | HBase table operation timeout in milliseconds. |
| `batchSize` | int|200| false | Batch size of updates made to the HBase table. |

### Example

Before using the HBase sink connector, you need to create a configuration file through one of the following methods.

* JSON 

    ```json
    {
        "hbaseConfigResources": "hbase-site.xml",
        "zookeeperQuorum": "localhost",
        "zookeeperClientPort": "2181",
        "zookeeperZnodeParent": "/hbase",
        "tableName": "pulsar_hbase",
        "rowKeyName": "rowKey",
        "familyName": "info",
        "qualifierNames": [ 'name', 'address', 'age']
    }
    ```


* YAML

    ```yaml
    configs:
        hbaseConfigResources: "hbase-site.xml"
        zookeeperQuorum: "localhost"
        zookeeperClientPort: "2181"
        zookeeperZnodeParent: "/hbase"
        tableName: "pulsar_hbase"
        rowKeyName: "rowKey"
        familyName: "info"
        qualifierNames: [ 'name', 'address', 'age']
    ```

    