---
id: version-2.3.0-io-netty
title: Netty Tcp or Udp Connector
sidebar_label: Netty Tcp or Udp Connector
original_id: io-netty
---

## Source

The Netty Source connector opens a port that accept incoming data via the configured network protocol and publish it to a user-defined Pulsar topic.
Also, this connector is suggested to be used in a containerized (e.g. k8s) deployment.
Otherwise, if the connector is running in process or thread mode, the instances may be conflicting on listening to ports.

### Source Configuration Options

| Name | Required | Default | Description |
|------|----------|---------|-------------|
| `type` | `false` | `tcp` | The network protocol over which data is trasmitted to netty. Valid values include HTTP, TCP, and UDP |
| `host` | `false` | `127.0.0.1` | The host name or address that the source instance to listen on. |
| `port` | `false` | `10999` | The port that the source instance to listen on. |
| `numberOfThreads` | `false` | `1` | The number of threads of Netty Tcp Server to accept incoming connections and handle the traffic of the accepted connections. |
