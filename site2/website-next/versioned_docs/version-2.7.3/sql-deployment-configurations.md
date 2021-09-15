---
id: sql-deployment-configurations
title: Pulsar SQL configuration and deployment
sidebar_label: Configuration and deployment
original_id: sql-deployment-configurations
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


You can configure Presto Pulsar connector and deploy a cluster with the following instruction.

## Configure Presto Pulsar Connector
You can configure Presto Pulsar Connector in the `${project.root}/conf/presto/catalog/pulsar.properties` properties file. The configuration for the connector and the default values are as follows.

```properties
# name of the connector to be displayed in the catalog
connector.name=pulsar

# the url of Pulsar broker service
pulsar.broker-service-url=http://localhost:8080

# URI of Zookeeper cluster
pulsar.zookeeper-uri=localhost:2181

# minimum number of entries to read at a single time
pulsar.entry-read-batch-size=100

# default number of splits to use per query
pulsar.target-num-splits=4
```

You can connect Presto to a Pulsar cluster with multiple hosts. To configure multiple hosts for brokers, add multiple URLs to `pulsar.broker-service-url`. To configure multiple hosts for ZooKeeper, add multiple URIs to `pulsar.zookeeper-uri`. The following is an example.
  
```
pulsar.broker-service-url=http://localhost:8080,localhost:8081,localhost:8082
pulsar.zookeeper-uri=localhost1,localhost2:2181
```

## Query data from existing Presto clusters

If you already have a Presto cluster, you can copy the Presto Pulsar connector plugin to your existing cluster. Download the archived plugin package with the following command.

```bash
$ wget pulsar:binary_release_url
```

## Deploy a new cluster

Since Pulsar SQL is powered by [Presto](https://prestosql.io), the configuration for deployment is the same for the Pulsar SQL worker. 

:::note

For how to set up a standalone single node environment, refer to [Query data](sql-getting-started.md). 

:::


You can use the same CLI args as the Presto launcher.

```bash
$ ./bin/pulsar sql-worker --help
Usage: launcher [options] command

Commands: run, start, stop, restart, kill, status

Options:
  -h, --help            show this help message and exit
  -v, --verbose         Run verbosely
  --etc-dir=DIR         Defaults to INSTALL_PATH/etc
  --launcher-config=FILE
                        Defaults to INSTALL_PATH/bin/launcher.properties
  --node-config=FILE    Defaults to ETC_DIR/node.properties
  --jvm-config=FILE     Defaults to ETC_DIR/jvm.config
  --config=FILE         Defaults to ETC_DIR/config.properties
  --log-levels-file=FILE
                        Defaults to ETC_DIR/log.properties
  --data-dir=DIR        Defaults to INSTALL_PATH
  --pid-file=FILE       Defaults to DATA_DIR/var/run/launcher.pid
  --launcher-log-file=FILE
                        Defaults to DATA_DIR/var/log/launcher.log (only in
                        daemon mode)
  --server-log-file=FILE
                        Defaults to DATA_DIR/var/log/server.log (only in
                        daemon mode)
  -D NAME=VALUE         Set a Java system property

```

The default configuration for the cluster is located in `${project.root}/conf/presto`. You can customize your deployment by modifying the default configuration.

You can set the worker to read from a different configuration directory, or set a different directory to write data. 

```bash
$ ./bin/pulsar sql-worker run --etc-dir /tmp/incubator-pulsar/conf/presto --data-dir /tmp/presto-1
```

You can start the worker as daemon process.

```bash
$ ./bin/pulsar sql-worker start
```

### Deploy a cluster on multiple nodes 

You can deploy a Pulsar SQL cluster or Presto cluster on multiple nodes. The following example shows how to deploy a cluster on three-node cluster. 

1. Copy the Pulsar binary distribution to three nodes.

The first node runs as Presto coordinator. The minimal configuration requirement in the `${project.root}/conf/presto/config.properties` file is as follows. 

```properties
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
query.max-memory=50GB
query.max-memory-per-node=1GB
discovery-server.enabled=true
discovery.uri=<coordinator-url>
```

The other two nodes serve as worker nodes, you can use the following configuration for worker nodes. 

```properties
coordinator=false
http-server.http.port=8080
query.max-memory=50GB
query.max-memory-per-node=1GB
discovery.uri=<coordinator-url>
```

2. Modify `pulsar.broker-service-url` and  `pulsar.zookeeper-uri` configuration in the `${project.root}/conf/presto/catalog/pulsar.properties` file accordingly for the three nodes.

3. Start the coordinator node.

```
$ ./bin/pulsar sql-worker run
```

4. Start worker nodes.

```
$ ./bin/pulsar sql-worker run
```

5. Start the SQL CLI and check the status of your cluster.

```bash
$ ./bin/pulsar sql --server <coordinate_url>
```

6. Check the status of your nodes.

```bash
presto> SELECT * FROM system.runtime.nodes;
 node_id |        http_uri         | node_version | coordinator | state  
---------+-------------------------+--------------+-------------+--------
 1       | http://192.168.2.1:8081 | testversion  | true        | active 
 3       | http://192.168.2.2:8081 | testversion  | false       | active 
 2       | http://192.168.2.3:8081 | testversion  | false       | active 
```

For more information about deployment in Presto, refer to [Presto deployment](https://prestosql.io/docs/current/installation/deployment.html).

:::note

The broker does not advance LAC, so when Pulsar SQL bypass broker to query data, it can only read entries up to the LAC that all the bookies learned. You can enable periodically write LAC on the broker by setting "bookkeeperExplicitLacIntervalInMills" in the broker.conf.

:::

