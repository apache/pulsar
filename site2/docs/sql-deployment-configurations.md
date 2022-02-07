---
id: sql-deployment-configurations
title: Pulsar SQL configuration and deployment
sidebar_label: Configuration and deployment
---

You can configure Presto Pulsar connector and deploy a cluster with the following instruction.

## Configure Presto Pulsar Connector
You can configure Presto Pulsar Connector in the `${project.root}/conf/presto/catalog/pulsar.properties` properties file. The configuration for the connector and the default values are as follows.

```properties
# name of the connector to be displayed in the catalog
connector.name=pulsar

# the url of Pulsar broker service
pulsar.web-service-url=http://localhost:8080

# URI of Zookeeper cluster
pulsar.zookeeper-uri=localhost:2181

# minimum number of entries to read at a single time
pulsar.entry-read-batch-size=100

# default number of splits to use per query
pulsar.target-num-splits=4

# max size of one batch message (default value is 5MB)
pulsar.max-message-size=5242880

# number of split used when querying data from pulsar
pulsar.target-num-splits=2

# size of queue to buffer entry read from pulsar
pulsar.max-split-entry-queue-size=1000

# size of queue to buffer message extract from entries
pulsar.max-split-message-queue-size=10000

# status provider to record connector metrics
pulsar.stats-provider=org.apache.bookkeeper.stats.NullStatsProvider

# config in map format for stats provider e.g. {"key1":"val1","key2":"val2"}
pulsar.stats-provider-configs={}

# whether to rewrite Pulsar's default topic delimiter '/'
pulsar.namespace-delimiter-rewrite-enable=false

# delimiter used to rewrite Pulsar's default delimiter '/', use if default is causing incompatibility with other system like Superset
pulsar.rewrite-namespace-delimiter=“/”

# maximum number of thread pool size for ledger offloader.
pulsar.managed-ledger-offload-max-threads=2

# driver used to offload or read cold data to or from long-term storage
pulsar.managed-ledger-offload-driver=null

# directory to load offloaders nar file.
pulsar.offloaders-directory="./offloaders"

# properties and configurations related to specific offloader implementation as map e.g. {"key1":"val1","key2":"val2"}
pulsar.offloader-properties={}

# authentication plugin used to authenticate to Pulsar cluster
pulsar.auth-plugin=null

# authentication parameter used to authenticate to the Pulsar cluster as a string e.g. "key1:val1,key2:val2".
pulsar.auth-params=null

# whether the Pulsar client accept an untrusted TLS certificate from broker
pulsar.tls-allow-insecure-connection=null

# whether to allow hostname verification when a client connects to broker over TLS.
pulsar.tls-hostname-verification-enable=null

# path for the trusted TLS certificate file of Pulsar broker
pulsar.tls-trust-cert-file-path=null

# set the threshold for BookKeeper request throttle, default is disabled
pulsar.bookkeeper-throttle-value=0

# set the number of IO thread
pulsar.bookkeeper-num-io-threads=2 * Runtime.getRuntime().availableProcessors()

# set the number of worker thread
pulsar.bookkeeper-num-worker-threads=Runtime.getRuntime().availableProcessors()

# whether to use BookKeeper V2 wire protocol
pulsar.bookkeeper-use-v2-protocol=true

# interval to check the need for sending an explicit LAC, default is disabled
pulsar.bookkeeper-explicit-interval=0

# size for managed ledger entry cache (in MB).
pulsar.managed-ledger-cache-size-MB=0

# number of threads to be used for managed ledger tasks dispatching
pulsar.managed-ledger-num-worker-threads=Runtime.getRuntime().availableProcessors()

# number of threads to be used for managed ledger scheduled tasks
pulsar.managed-ledger-num-scheduler-threads=Runtime.getRuntime().availableProcessors()

# directory used to store extraction NAR file
pulsar.nar-extraction-directory=System.getProperty("java.io.tmpdir")
```
You can connect Presto to a Pulsar cluster with multiple hosts. To configure multiple hosts for brokers, add multiple URLs to `pulsar.web-service-url`. To configure multiple hosts for ZooKeeper, add multiple URIs to `pulsar.zookeeper-uri`. The following is an example.
  
```
pulsar.web-service-url=http://localhost:8080,localhost:8081,localhost:8082
pulsar.zookeeper-uri=localhost1,localhost2:2181
```

A frequently asked question is why my latest message not showing up when querying with Pulsar SQL.
It's not a bug but controlled by a setting, by default BookKeeper LAC only advanced when subsequent entries are added.
If there is no subsequent entries added, the last entry written will not be visible to readers until the ledger is closed.
This is not a problem for Pulsar which uses managed ledger, but Pulsar SQL directly read from BookKeeper ledger.
We can add following setting to change the behavior:
In Broker config, set
bookkeeperExplicitLacIntervalInMills > 0
bookkeeperUseV2WireProtocol=false

And in Presto config, set
pulsar.bookkeeper-explicit-interval > 0
pulsar.bookkeeper-use-v2-protocol=false

However,keep in mind that using bk V3 protocol will introduce additional GC overhead to BK as it uses Protobuf.

## Query data from existing Presto clusters

If you already have a Presto cluster, you can copy the Presto Pulsar connector plugin to your existing cluster. Download the archived plugin package with the following command.

```bash
$ wget pulsar:binary_release_url
```

## Deploy a new cluster

Since Pulsar SQL is powered by [Trino (formerly Presto SQL)](https://trino.io), the configuration for deployment is the same for the Pulsar SQL worker. 

> Note  
> For how to set up a standalone single node environment, refer to [Query data](sql-getting-started.md). 

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

2. Modify `pulsar.web-service-url` and  `pulsar.zookeeper-uri` configuration in the `${project.root}/conf/presto/catalog/pulsar.properties` file accordingly for the three nodes.

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

For more information about deployment in Presto, refer to [Presto deployment](https://trino.io/docs/current/installation/deployment.html).

> Note  
> The broker does not advance LAC, so when Pulsar SQL bypass broker to query data, it can only read entries up to the LAC that all the bookies learned. You can enable periodically write LAC on the broker by setting "bookkeeperExplicitLacIntervalInMills" in the broker.conf.
