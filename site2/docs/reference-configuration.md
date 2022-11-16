---
id: reference-configuration
title: Pulsar Configuration
sidebar_label: "Pulsar Configuration"
---

For a complete list of Pulsar configuration, visit the [Pulsar Reference](http://pulsar.apache.org/reference/#/) website.

***

### Override client configurations

If you want to override the configurations of clients internal to brokers, websockets, and proxies, you can use the following prefix.

|Prefix|Description|
|---|---|
|brokerClient_| Configure **all** the Pulsar Clients and Pulsar Admin Clients of brokers, websockets, and proxies. These configurations are applied after hard-coded configurations and before the client configurations named in the [Pulsar Reference](http://pulsar.apache.org/reference/#/) website.|
|bookkeeper_| Configure the broker's BookKeeper clients used by managed ledgers and the BookkeeperPackagesStorage bookkeeper client. Takes precedence over most other configuration values.|

:::note
* This feature only applies to Pulsar 2.10.1 and later versions.
* When running the function worker within the broker, you have to configure those clients by using the `functions_worker.yml` file. These prefixed configurations do not apply to any of those clients.
:::