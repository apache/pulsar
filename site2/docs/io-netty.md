---
id: io-netty
title: Netty Tcp or Udp Connector
sidebar_label: Netty Tcp or Udp Connector
---

## Source

The Netty Tcp or Udp Source connector is used to listen Tcp/Udp messages from Tcp/Udp Client and write them to user-defined Pulsar topic.
Also, this connector is suggested to be used in a containerized (e.g. k8s) deployment.
Otherwise, if the connector is running in process or thread mode, the instances may be conflicting on listening to ports.

### Source Configuration Options

| Name | Required | Default | Description |
|------|----------|---------|-------------|
| `type` | `false` | `tcp` | The tcp or udp network protocol required by netty. |
| `host` | `false` | `127.0.0.1` | The host name or address that the source instance to listen on. |
| `port` | `false` | `10999` | The port that the source instance to listen on. |
| `numberOfThreads` | `false` | `1` | The number of threads of Netty Tcp Server to accept incoming connections and handle the traffic of the accepted connections. |
