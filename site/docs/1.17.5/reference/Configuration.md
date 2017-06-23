---
title: Pulsar configuration
---

Pulsar configuration can be managed either via a series of configuration files contained in the [`conf`]({{ site.pulsar_repo }}/conf) directory of a Pulsar [installation](../../getting-started/LocalCluster)

* [BookKeeper](#bookkeeper)
* [Broker](#broker)
* [Client](#client)
* [Discovery](#discovery)
* [Global ZooKeeper](#global-zookeeper)
* [Log4j](#log4j)
* [Log4j shell](#log4j-shell)
* [Standalone](#standalone)
* [WebSocket](#websocket)
* [ZooKeeper](#zookeeper)

## BookKeeper

{% popover BookKeeper %} is a replicated log storage system that Pulsar uses for durable storage of all messages.

{% include config.html id="bookkeeper" %}

## Broker

Pulsar {% popover brokers %} are responsible for handling incoming messages from {% popover producers %}, dispatching messages to {% popover consumers %}, replicating data between {% popover clusters %}, and more.

{% include config.html id="broker" %}

## Client

The [`pulsar-client`](../CliTools#pulsar-client) CLI tool can be used to publish messages to Pulsar and consume messages from Pulsar {% popover topics %}. This tool can be used in lieu of a client library.

{% include config.html id="client" %}

## Service discovery

{% include config.html id="discovery" %}

## Global ZooKeeper

{% include config.html id="global-zookeeper" %}

## Log4j

{% include config.html id="log4j" %}

## Log4j shell

{% include config.html id="log4j-shell" %}

## Standalone

{% include config.html id="standalone" %}

## WebSocket

{% include config.html id="websocket" %}

## ZooKeeper

{% popover ZooKeeper %} handles a broad range of essential configuration- and coordination-related tasks for Pulsar. The default configuration file for ZooKeeper is in the `conf/zookeeper.conf` file in your Pulsar installation. The following parameters are available:

{% include config.html id="zookeeper" %}

In addition to the parameters in the table above, configuring ZooKeeper for Pulsar involves adding a
a `server.N` line to the `conf/zookeeper.conf` file for each node in the ZooKeeper cluster, where `N` is the number of the ZooKeeper node. Here's an example for a three-node ZooKeeper cluster:

```properties
server.1=zk1.us-west.example.com:2888:3888
server.2=zk2.us-west.example.com:2888:3888
server.3=zk3.us-west.example.com:2888:3888
```

{% include admonition.html type="info" content="
We strongly recommend consulting the [ZooKeeper Administrator's Guide](https://zookeeper.apache.org/doc/current/zookeeperAdmin.html) for a more thorough and comprehensive introduction to ZooKeeper configuration." %}
