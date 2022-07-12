---
id: administration-metadata-store
title: Configure metadata store
sidebar_label: "Configure metadata store"
---

Pulsar metadata store maintains all the metadata, configuration, and coordination of a Pulsar cluster, such as topic metadata, schema, broker load data, and so on. 

The metadata store of each Pulsar instance should contain the following two components:
* A local metadata store ensemble (`metadataStoreUrl`) that stores cluster-specific configuration and coordination, such as which brokers are responsible for which topics as well as ownership metadata, broker load reports, and BookKeeper ledger metadata.
* A configuration store quorum (`configurationMetadataStoreUrl`) stores configuration for clusters, tenants, namespaces, topics, and other entities that need to be globally consistent.

:::note

If you are using a standalone Pulsar or a single Pulsar cluster, you only need to configure one metadata store (via `metadataStoreUrl`) and it also serves as a configuration store.

:::

Pulsar supports the following metadata store services:
* [Apache ZooKeeper](https://zookeeper.apache.org/)
* [Etcd](https://etcd.io/)
* [RocksDB](http://rocksdb.org/)
* Local memory

:::note

RocksDB and local memory are only applicable to standalone Pulsar or single-node Pulsar clusters.

:::

## Use ZooKeeper as metadata store

Pulsar metadata store can be deployed on a separate ZooKeeper cluster or deployed on an existing ZooKeeper cluster.

To use ZooKeeper as the metadata store, add the following parameters to the `conf/broker.conf` or `conf/standalone.conf` file.

```conf

metadataStoreUrl=zk:my-zk-1:2181,my-zk-2:2181,my-zk-3:2181
configurationMetadataStoreUrl=zk:my-global-zk-1:2181,my-global-zk-2:2181,my-global-zk-3:2181

```

## Use etcd as metadata store

To use etcd as the metadata store, add the following parameters to the `conf/broker.conf` or `conf/standalone.conf` file.

```conf

metadataStoreUrl=etcd:my-etcd-1:2379,my-etcd-2:2379,my-etcd-3:2379
configurationMetadataStoreUrl=etcd:my-global-etcd-1:2379,my-global-etcd-2:2379,my-global-etcd-3:2379
# metadataStoreConfigPath=/path/to/file

```

:::tip

The `metadataStoreConfigPath` parameter is required when you want to use the following advanced configurations.

```
useTls=false
tlsProvider=JDK
tlsTrustCertsFilePath=
tlsKeyFilePath=
tlsCertificateFilePath=
authority=
```

:::

## Use RocksDB as metadata store

To use RocksDB as the metadata store, add the following parameters to the `conf/broker.conf` or `conf/standalone.conf` file.

```conf

metadataStoreUrl=rocksdb://data/metadata
# metadataStoreConfigPath=/path/to/file

```

:::tip

The `metadataStoreConfigPath` parameter is required when you want to use advanced configurations. See [this example](https://github.com/facebook/rocksdb/blob/main/examples/rocksdb_option_file_example.ini) for more information.

:::

## Use local memory as metadata store

To use local memory as the metadata store, add the following parameters to the `conf/broker.conf` or `conf/standalone.conf` file.

```conf

metadataStoreUrl=memory://local

```


## Enable batch operations on metadata store

Pulsar metadata store supports batch operations and caching to meet low latency and high throughput and improve performance. 

To enable batch operations on the metadata store, you can configure the following parameters in the `conf/broker.conf` or `conf/standalone.conf` file.

```conf

# Whether we should enable metadata operations batching
metadataStoreBatchingEnabled=true

# Maximum delay to impose on batching grouping
metadataStoreBatchingMaxDelayMillis=5

# Maximum number of operations to include in a singular batch
metadataStoreBatchingMaxOperations=1000

# Maximum size of a batch
metadataStoreBatchingMaxSizeKb=128

```

