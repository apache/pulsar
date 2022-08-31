---
id: sql-deployment-configurations
title: Pulsar SQl Deployment and Configuration
sidebar_label: "Deployment and Configuration"
original_id: sql-deployment-configurations
---

Below is a list configurations for the Presto Pulsar connector and instruction on how to deploy a cluster.

## Presto Pulsar Connector Configurations
There are several configurations for the Presto Pulsar Connector.  The properties file that contain these configurations can be found at ```${project.root}/conf/presto/catalog/pulsar.properties```.
The configurations for the connector and its default values are described below.

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

## Query Pulsar from Existing Presto Cluster

If you already have an existing Presto cluster, you can copy Presto Pulsar connector plugin to your existing cluster.  You can download the archived plugin package via:

```bash

$ wget pulsar:binary_release_url

```

## Deploying a new cluster

Please note that the [Getting Started](sql-getting-started) guide shows you how to easily setup a standalone single node environment to experiment with.

Pulsar SQL is powered by [Presto](https://prestosql.io) thus many of the configurations for deployment is the same for the Pulsar SQL worker.

You can use the same CLI args as the Presto launcher:

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

There is a set of default configs for the cluster located in ```${project.root}/conf/presto``` that will be used by default.  You can change them to customize your deployment

You can also set the worker to read from a different configuration directory as well as set a different directory for writing its data:

```bash

$ ./bin/pulsar sql-worker run --etc-dir /tmp/incubator-pulsar/conf/presto --data-dir /tmp/presto-1

```

You can also start the worker as daemon process:

```bash

$ ./bin/pulsar sql-worker start

```

### Deploying to a 3 node cluster

For example, if I wanted to deploy a Pulsar SQL/Presto cluster on 3 nodes, you can do the following:

First, copy the Pulsar binary distribution to all three nodes.

The first node, will run the Presto coordinator.  The minimal configuration in ```${project.root}/conf/presto/config.properties``` can be the following

```properties

coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
query.max-memory=50GB
query.max-memory-per-node=1GB
discovery-server.enabled=true
discovery.uri=<coordinator-url>

```

Also, modify ```pulsar.broker-service-url``` and  ```pulsar.zookeeper-uri``` configs in ```${project.root}/conf/presto/catalog/pulsar.properties``` on those nodes accordingly

Afterwards, you can start the coordinator by just running

```$ ./bin/pulsar sql-worker run```

For the other two nodes that will only serve as worker nodes, the configurations can be the following:

```

properties
coordinator=false
http-server.http.port=8080
query.max-memory=50GB
query.max-memory-per-node=1GB
discovery.uri=<coordinator-url>

```

Also, modify

```

pulsar.broker-service-url``` and  ```pulsar.zookeeper-uri``` configs in ```${project.root}/conf/presto/catalog/pulsar.properties``` accordingly

You can also start the worker by just running: ```$ ./bin/pulsar sql-worker run```

You can check the status of your cluster from the SQL CLI.  To start the SQL CLI:

```bash

$ ./bin/pulsar sql --server <coordinate_url>

```

You can then run the following command to check the status of your nodes:

```bash

presto> SELECT * FROM system.runtime.nodes;
 node_id |        http_uri         | node_version | coordinator | state  
---------+-------------------------+--------------+-------------+--------
 1       | http://192.168.2.1:8081 | testversion  | true        | active 
 3       | http://192.168.2.2:8081 | testversion  | false       | active 
 2       | http://192.168.2.3:8081 | testversion  | false       | active

```

For more information about deployment in Presto, please reference:

[Deploying Presto](https://prestosql.io/docs/current/installation/deployment.html)

