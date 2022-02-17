---
id: version-2.2.1-io-tcp
title: Netty Tcp Connector
sidebar_label: Netty Tcp Connector
original_id: io-tcp
---

## Source

The Netty Tcp Source connector is used to listen Tcp messages from Tcp Client and write them to user-defined Pulsar topic.
Also, this connector is suggested to be used in a containerized (e.g. k8s) deployment.
Otherwise, if the connector is running in process or thread mode, the instances may be conflicting on listening to ports.

### Source Configuration Options

| Name | Required | Default | Description |
|------|----------|---------|-------------|
| `host` | `false` | `127.0.0.1` | The host name or address that the source instance to listen on. |
| `port` | `false` | `10999` | The port that the source instance to listen on. |
| `numberOfThreads` | `false` | `1` | The number of threads of Netty Tcp Server to accept incoming connections and handle the traffic of the accepted connections. |
