# ZooKeeper

ZooKeeper handles a broad range of essential configuration- and coordination-related tasks for Pulsar. The default configuration file for ZooKeeper is in the `conf/zookeeper.conf` file in your Pulsar installation. The following parameters are available:

### tickTime

The tick is the basic unit of time in ZooKeeper, measured in milliseconds and used to regulate things like heartbeats and timeouts. tickTime is the length of a single tick.

**Default**: 2000

### initLimit

The maximum time, in ticks, that the leader ZooKeeper server allows follower ZooKeeper servers to successfully connect and sync. The tick time is set in milliseconds using the tickTime parameter.

**Default**: 10

### syncLimit

The maximum time, in ticks, that a follower ZooKeeper server is allowed to sync with other ZooKeeper servers. The tick time is set in milliseconds using the tickTime parameter.

**Default**: 5

### dataDir

The location where ZooKeeper will store in-memory database snapshots as well as the transaction log of updates to the database.

**Default**: data/zookeeper

### clientPort

The port on which the ZooKeeper server will listen for connections.

**Default**: 2181

### admin.enableServer

The port at which the admin listens.

**Default**: true

### admin.serverPort

The port at which the admin listens.

**Default**: 9990

### autopurge.snapRetainCount

In ZooKeeper, auto purge determines how many recent snapshots of the database stored in dataDir to retain within the time interval specified by autopurge.purgeInterval (while deleting the rest).

**Default**: 3

### autopurge.purgeInterval

The time interval, in hours, by which the ZooKeeper database purge task is triggered. Setting to a non-zero number will enable auto purge; setting to 0 will disable. Read this guide before enabling auto purge.

**Default**: 1

### forceSync

Requires updates to be synced to media of the transaction log before finishing processing the update. If this option is set to 'no', ZooKeeper will not require updates to be synced to the media. WARNING: it's not recommended to run a production ZK cluster with `forceSync` disabled.

**Default**: yes

### maxClientCnxns

The maximum number of client connections. Increase this if you need to handle more ZooKeeper clients.

**Default**: 60

---

In addition to the parameters above, configuring ZooKeeper for Pulsar involves adding a `server.N` line to the `conf/zookeeper.conf` file for each node in the ZooKeeper cluster, where `N` is the number of the ZooKeeper node. Here's an example of a three-node ZooKeeper cluster:

```properties

server.1=zk1.us-west.example.com:2888:3888
server.2=zk2.us-west.example.com:2888:3888
server.3=zk3.us-west.example.com:2888:3888

```

> We strongly recommend consulting the [ZooKeeper Administrator's Guide](https://zookeeper.apache.org/doc/current/zookeeperAdmin.html) for a more thorough and comprehensive introduction to ZooKeeper configuration
