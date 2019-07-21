---
title: Deploying a Pulsar instance on bare metal
tags: [admin, deployment, instance, bare metal]
---

<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

{% include admonition.html type="info"
  content="Single-cluster Pulsar installations should be sufficient for all but the most ambitious use cases. If you're interested in experimenting with Pulsar or using it in a startup or on a single team, we recommend opting for a single cluster. For instructions on deploying a single cluster, see the guide [here](../cluster)." %}

A Pulsar *instance* consists of multiple Pulsar {% popover clusters %} working in unison. Clusters can be distributed across data centers or geographical regions and can replicate amongst themselves using [geo-replication](../../admin/GeoReplication). Deploying a multi-cluster Pulsar instance involves the following basic steps:

* Deploying two separate [ZooKeeper](#deploying-zookeeper) quorums: a [local](#deploying-local-zookeeper) quorum for each cluster in the instance and a [configuration store](#configuration-store) quorum for instance-wide tasks
* Initializing [cluster metadata](#cluster-metadata-initialization) for each cluster
* Deploying a [BookKeeper cluster](#deploying-bookkeeper) of {% popover bookies %} in each Pulsar cluster
* Deploying [brokers](#deploying-brokers) in each Pulsar cluster

If you're deploying a single Pulsar cluster, see the [Clusters and Brokers](../../getting-started/LocalCluster#starting-the-cluster) guide.

{% include admonition.html type="info" title='Running Pulsar locally or on Kubernetes?' content="
This guide shows you how to deploy Pulsar in production in a non-Kubernetes. If you'd like to run a standalone Pulsar cluster on a single machine for development purposes, see the [Setting up a local cluster](../../getting-started/LocalCluster) guide. If you're looking to run Pulsar on [Kubernetes](https://kubernetes.io), see the [Pulsar on Kubernetes](../Kubernetes) guide, which includes sections on running Pulsar on Kubernetes on [Google Kubernetes Engine](../Kubernetes#pulsar-on-google-kubernetes-engine) and on [Amazon Web Services](../Kubernetes#pulsar-on-amazon-web-services).
" %}

{% include explanations/install-package.md %}

## Deploying ZooKeeper

{% include explanations/deploying-zk.md %}

## Cluster metadata initialization

Once you've set up the cluster-specific ZooKeeper and {% popover configuration store %} quorums for your instance, there is some metadata that needs to be written to ZooKeeper for each cluster in your instance. **It only needs to be written once**.

You can initialize this metadata using the [`initialize-cluster-metadata`](../../reference/CliTools#pulsar-initialize-cluster-metadata) command of the [`pulsar`](../../reference/CliTools#pulsar) CLI tool. Here's an example:

```shell
$ bin/pulsar initialize-cluster-metadata \
  --cluster us-west \
  --zookeeper zk1.us-west.example.com:2181 \
  --configuration-store zk1.us-west.example.com:2184 \
  --web-service-url http://pulsar.us-west.example.com:8080/ \
  --web-service-url-tls https://pulsar.us-west.example.com:8443/ \
  --broker-service-url pulsar://pulsar.us-west.example.com:6650/ \
  --broker-service-url-tls pulsar+ssl://pulsar.us-west.example.com:6651/
```

As you can see from the example above, the following needs to be specified:

* The name of the cluster
* The local ZooKeeper connection string for the cluster
* The {% popover configuration store %} connection string for the entire instance
* The web service URL for the cluster
* A broker service URL enabling interaction with the {% popover brokers %} in the cluster

If you're using [TLS](../../security/tls), you'll also need to specify a TLS web service URL for the cluster as well as a TLS broker service URL for the brokers in the cluster.

Make sure to run `initialize-cluster-metadata` for each cluster in your instance.

## Deploying BookKeeper

{% include explanations/deploying-bk.md %}

## Deploying brokers

Once you've set up ZooKeeper, initialized cluster metadata, and spun up BookKeeper {% popover bookies %}, you can deploy {% popover brokers %}.

### Broker configuration

Brokers can be configured using the [`conf/broker.conf`](../../reference/Configuration#broker) configuration file.

The most important element of broker configuration is ensuring that each broker is aware of its local ZooKeeper quorum as well as the configuration store quorum. Make sure that you set the [`zookeeperServers`](../../reference/Configuration#broker-zookeeperServers) parameter to reflect the local quorum and the [`configurationStoreServers`](../../reference/Configuration#broker-configurationStoreServers) parameter to reflect the configuration store quorum (although you'll need to specify only those ZooKeeper servers located in the same cluster).

You also need to specify the name of the {% popover cluster %} to which the broker belongs using the [`clusterName`](../../reference/Configuration#broker-clusterName) parameter.

Here's an example configuration:

```properties
# Local ZooKeeper servers
zookeeperServers=zk1.us-west.example.com:2181,zk2.us-west.example.com:2181,zk3.us-west.example.com:2181

# Configuration store quorum connection string.
configurationStoreServers=zk1.us-west.example.com:2184,zk2.us-west.example.com:2184,zk3.us-west.example.com:2184

clusterName=us-west
```

### Broker hardware

Pulsar brokers do not require any special hardware since they don't use the local disk. Fast CPUs and 10Gbps [NIC](https://en.wikipedia.org/wiki/Network_interface_controller) are recommended since the software can take full advantage of that.

### Starting the broker service

You can start a broker in the background using [nohup](https://en.wikipedia.org/wiki/Nohup) with the [`pulsar-daemon`](../../reference/CliTools#pulsar-daemon) CLI tool:

```shell
$ bin/pulsar-daemon start broker
```

You can also start brokers in the foreground using [`pulsar broker`](../../reference/CliTools#pulsar-broker):

```shell
$ bin/pulsar broker
```

## Service discovery

{% include explanations/service-discovery-setup.md %}

## Admin client and verification

At this point your Pulsar instance should be ready to use. You can now configure client machines that can serve as [administrative clients](../../admin-api/overview) for each cluster. You can use the [`conf/client.conf`](../../reference/Configuration#client) configuration file to configure admin clients.

The most important thing is that you point the [`serviceUrl`](../../reference/Configuration#client-serviceUrl) parameter to the correct service URL for the cluster:

```properties
serviceUrl=http://pulsar.us-west.example.com:8080/
```

## Provisioning new tenants

Pulsar was built as a fundamentally {% popover multi-tenant %} system.

To allow a new {% popover tenant %} to use the system, we need to create a new one. You can create a new tenant using the [`pulsar-admin`](../../reference/CliTools#pulsar-admin-tenants-create) CLI tool:

```shell
$ bin/pulsar-admin tenants create test-tentant \
  --allowed-clusters us-west \
  --admin-roles test-admin-role
```

This will allow users who identify with role `test-admin-role` to administer the configuration for the tenant `test` which will only be allowed to use the cluster `us-west`. From now on, this tenant will be able to self-manage its resources.

Once a tenant has been created, you will need to create {% popover namespaces %} for topics within that tenant.

The first step is to create a namespace. A namespace is an administrative unit that can contain many topics. A common practice is to create a namespace for each different use case from a single tenant.

```shell
$ bin/pulsar-admin namespaces create test-tenant/ns1
```

##### Testing producer and consumer

Everything is now ready to send and receive messages. The quickest way to test
the system is through the `pulsar-perf` client tool.

Let's use a topic in the namespace we just created. Topics are automatically
created the first time a producer or a consumer tries to use them.

The topic name in this case could be:

{% include topic.html ten="test-tenant" n="ns1" t="my-topic" %}

Start a consumer that will create a subscription on the topic and will wait
for messages:

```shell
$ bin/pulsar-perf consume persistent://test-tenant/us-west/ns1/my-topic
```

Start a producer that publishes messages at a fixed rate and report stats every
10 seconds:

```shell
$ bin/pulsar-perf produce persistent://test-tenant/us-west/ns1/my-topic
```

To report the topic stats:

```shell
$ bin/pulsar-admin persistent stats persistent://test-tenant/us-west/ns1/my-topic
```
