# Pulsar Cluster Setup

<!-- TOC depthFrom:2 depthTo:4 withLinks:1 updateOnSave:1 orderedList:0 -->

- [Setup](#setup)
	- [System requirements](#system-requirements)
	- [Components](#components)
		- [ZooKeeper](#zookeeper)
		- [Global ZooKeeper](#global-zookeeper)
		- [Cluster metadata initialization](#cluster-metadata-initialization)
		- [BookKeeper](#bookkeeper)
		- [Broker](#broker)
		- [Service discovery](#service-discovery)
		- [Admin client and verification](#admin-client-and-verification)
- [Monitoring](#monitoring)

<!-- /TOC -->

## Setup

### System requirements

Supported platforms:
  * Linux
  * MacOS X

Required software:
  * Java 1.8

### Components

#### ZooKeeper

Add all ZK servers the quorum configuration. Edit `conf/zookeeper.conf` and add
the following lines in all the ZK servers:

```
server.1=zk1.us-west.example.com:2888:3888
server.2=zk2.us-west.example.com:2888:3888
server.3=zk3.us-west.example.com:2888:3888
...
```

Start ZK service on all the hosts:

```shell
$ bin/pulsar-daemon start zookeeper
```

#### Global ZooKeeper

Configure the global quorum by adding the participants and all the observers.

##### Single cluster pulsar instance

When deploying a pulsar instance with a single cluster, the global zookeeper can
be deployed in the same machines as the _local_ ZK quorum, running on different
TCP ports. Add the servers in `conf/global_zookeeper.conf`, to start the service
on port `2184`:

```
clientPort=2184
server.1=zk1.us-west.example.com:2185:2186
server.2=zk2.us-west.example.com:2185:2186
server.3=zk3.us-west.example.com:2185:2186
...
```

##### Multi-cluster pulsar instance

When deploying a global Pulsar instance, with clusters distributed across
different geographical regions, the global ZooKeeper serves as a highly-available
and strongly-consistent metadata store that can tolerate whole regions failures and
partitions.

The key here is to make sure the ZK quorum members are spread across at least 3
regions and that other regions are running as observers.

Again, given the very low expected load on the global ZooKeeper servers, we can
share the same hosts used for the local ZooKeeper quorum.

For example, let's assume a Pulsar instance with the following clusters `us-west`,
`us-east`, `us-central`, `eu-central`, `ap-south`. Also let's assume, each cluster
will have its own local ZK servers named such as
```
zk[1-3].${CLUSTER}.example.com
```

In this scenario we want to pick the quorum participants from few clusters and
let all the others be ZK observers. For example, to form a 7 servers quorum, we
can pick 3 servers from `us-west`, 2 from `us-central` and 2 from `us-east`.

This will guarantee that writes to global ZooKeeper will be possible even if one
of these regions is unreachable.

The ZK configuration in all the servers will look like:

```
clientPort=2184
server.1=zk1.us-west.example.com:2185:2186
server.2=zk2.us-west.example.com:2185:2186
server.3=zk3.us-west.example.com:2185:2186
server.4=zk1.us-central.example.com:2185:2186
server.5=zk2.us-central.example.com:2185:2186
server.6=zk3.us-central.example.com:2185:2186:observer
server.7=zk1.us-east.example.com:2185:2186
server.8=zk2.us-east.example.com:2185:2186
server.9=zk3.us-east.example.com:2185:2186:observer
server.10=zk1.eu-central.example.com:2185:2186:observer
server.11=zk2.eu-central.example.com:2185:2186:observer
server.12=zk3.eu-central.example.com:2185:2186:observer
server.13=zk1.ap-south.example.com:2185:2186:observer
server.14=zk2.ap-south.example.com:2185:2186:observer
server.15=zk3.ap-south.example.com:2185:2186:observer
```

Additionally, ZK observers will need to have :

```
peerType=observer
```

##### Starting the service

```shell
$ bin/pulsar-daemon start global-zookeeper
```

#### Cluster metadata initialization

When setting up a new cluster, there is some metadata that needs to be initialized
for the first time. The following command will prepare both the BookKeeper
as well as the Pulsar metadata.

```shell
$ bin/pulsar initialize-cluster-metadata --cluster us-west \
                                         --zookeeper zk1.us-west.example.com:2181 \
                                         --global-zookeeper zk1.us-west.example.com:2184 \
                                         --web-service-url http://pulsar.us-west.example.com:8080/ \
                                         --web-service-url-tls https://pulsar.us-west.example.com:8443/ \
                                         --broker-service-url pulsar://pulsar.us-west.example.com:6650/ \
                                         --broker-service-url-tls pulsar+ssl://pulsar.us-west.example.com:6651/
```

#### BookKeeper

Bookie hosts are responsible for storing the data on disk and it's very important
to have a suitable hardware configuration to ensure good performance.

There are two key dimensions for capacity:

- Disk I/O capacity read/write
- Storage capacity

Entries written to a Bookie are always synced on disk before return an
acknowledgment to the Pulsar broker. To ensure low write latency, BookKeeper is
designed to use multiple devices:

- a _journal_ to ensure durability

  - It is critical to have fast _fsync_ operation on this device for sequential
  writes. Typically, a small and fast SSDs will be fine, or HDDs with RAID
	controller and battery backed write cache. Both solutions can reach fsync
  latency of ~0.4 ms.

- the _"Ledger storage device"_

  - This is where data is stored until all the consumers have acknowledge the
  messages. Writes will happen in background, so write IO is not a big concern.
  Reads will happen sequentially most of the type and only in case some consumer
  is draining backlog. Typical configuration will use multiple HDDs with RAID
  controller, to be able to store large amounts of data

##### Configuration

Minimum changes required to configuration in
`conf/bookkeeper.conf` are:

```shell
# Change to point to journal disk mount point
journalDirectory=data/bookkeeper/journal

# Point to ledger storage disk mount point
ledgerDirectories=data/bookkeeper/ledgers

# Point to local ZK quorum
zkServers=zk1.example.com:2181,zk2.example.com:2181,zk3.example.com:2181

# Change the ledger manager type
ledgerManagerType=hierarchical
```

Please consult http://bookkeeper.apache.org/ for more extensive documentation
on Apache BookKeeper.

##### Starting the service

Start the bookie:
```shell
$ bin/pulsar-daemon start bookie
```

Verify the bookie is working properly:

```shell
$ bin/bookkeeper shell bookiesanity
```

This will create a new ledger on the local bookie, write few entries, read
them back and finally delete the ledger.

#### Broker

Pulsar brokers do not need any special hardware consideration since they don't
use the local disk. Fast CPUs and 10Gbps NIC are recommended since the software
can take full advantage of that.

Minimal configuration changes in `conf/broker.conf` will include:

```shell
# Local ZK servers
zookeeperServers=zk1.us-west.example.com:2181,zk2.us-west.example.com:2181,zk3.us-west.example.com:2181

# Global Zookeeper quorum connection string. Here we just need to specify the
# servers located in the same cluster
globalZookeeperServers=zk1.us-west.example.com:2184,zk2.us-west.example.com:2184,zk3.us-west.example.com:2184

clusterName=us-west
```

##### Start broker service

```shell
$ bin/pulsar-daemon start broker
```

#### Service discovery

Service discovery component is used to give a single URL for the clients to use.

You can either use the provided `discovery-service` or any other method. The
only requirement is that when the client does a HTTP request on
`http://pulsar.us-west.example.com:8080/` it must be redirected (through DNS, IP
or HTTP redirect) to an active broker, without preference.

The included discovery service maintains the list of active brokers from ZooKeeper and it supports lookup redirection with HTTP and also with [binary protocol](https://github.com/yahoo/pulsar/blob/master/docs/BinaryProtocol.md#service-discovery).

Add the ZK servers in `conf/discovery.conf`:
```shell
# Zookeeper quorum connection string
zookeeperServers=zk1.us-west.example.com:2181,zk2.us-west.example.com:2181,zk3.us-west.example.com:2181

# Global zookeeper quorum connection string
globalZookeeperServers=zk1.us-west.example.com:2184,zk2.us-west.example.com:2184,zk3.us-west.example.com:2184
```

Start the service:

```
$ bin/pulsar-daemon start discovery
```

#### Admin client and verification

At this point the cluster should be ready to use. We can now configure a client
machine that can serve as the administrative client.

Edit `conf/client.conf` to point the client to the correct service URL:

```shell
serviceUrl=http://pulsar.us-west.example.com:8080/
```

##### Provisioning a new tenant

To allow a new tenant to use the system, we need to create a new property.
Typically this will be done by the Pulsar cluster administrator or by some
automated tool:

```shell
$ bin/pulsar-admin properties create test \
                --allowed-clusters us-west \
                --admin-roles test-admin-role
```

This will allow users who identify with role `test-admin-role` to administer
the configuration for the property `test` which will only be allowed to use the
cluster `us-west`.

The tenant will be able from now on to self manage its resources.

The first step is to create a namespace. A namespace is an administrative unit
that can contain many topic. Common practice is to create a namespace for each
different use case from a single tenant.

```shell
$ bin/pulsar-admin namespaces create test/us-west/ns1
```

##### Testing producer and consumer

Everything is now ready to send and receive messages. The quickest way to test
the system is through the `pulsar-perf` client tool.

Let's use a topic in the namespace we just created. Topics are automatically
created the first time a producer or a consumer tries to use them.

The topic name in this case could be:
```
persistent://test/us-west/ns1/my-topic
```

Start a consumer that will create a subscription on the topic and will wait
for messages:

```shell
$ bin/pulsar-perf consume persistent://test/us-west/ns1/my-topic
```

Start a producer that publishes messages at a fixed rate and report stats every
10 seconds:

```shell
$ bin/pulsar-perf produce persistent://test/us-west/ns1/my-topic
```

To report the topic stats:
```shell
$ bin/pulsar-admin persistent stats persistent://test/us-west/ns1/my-topic
```


--------------------------------------------------------------------------------

## Monitoring

### Broker stats

Pulsar broker metrics can be collected from the brokers and are exported in JSON format.

There are two main types of metrics:

* Destination dump, containing stats for each individual topic
```shell
bin/pulsar-admin broker-stats destinations
```

* Broker metrics, containing broker info and topics stats aggregated at namespace
  level:
```shell
bin/pulsar-admin broker-stats monitoring-metrics
```

All the message rates are updated every 1min.

### BookKeeper stats

There are several stats frameworks that works with BookKeeper and that
can be enabled by changing the `statsProviderClass` in
`conf/bookkeeper.conf`.

By following the instructions above, the `DataSketchesMetricsProvider`
will be enabled. It features a very efficient way to compute latency
quantiles, along with rates and counts.

The stats are dumped every interval into a JSON file that is overwritten
each time.

```properties
statsProviderClass=org.apache.bokkeeper.stats.datasketches.DataSketchesMetricsProvider
dataSketchesMetricsJsonFileReporter=data/bookie-stats.json
dataSketchesMetricsUpdateIntervalSeconds=60
```
