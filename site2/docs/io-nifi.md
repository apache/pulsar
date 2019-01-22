---
id: io-nifi
title: nifi Connector
sidebar_label: nifi Connector
---

## Source

The NiFi Source Connector is used move messages from Apache NiFi using the NiFi Site-to-Site client and persist the messages to a Pulsar topic.

### Source Configuration Options

| Name | Required | Default | Description |
|------|----------|---------|-------------|
| url | `true` | `null` | Specifies the URL of the remote NiFi instance. |
| portName | `true` | `null` | Specifies the name of the port to communicate with. Either the port name or the port identifier must be specified. |
| requestBatchCount | `false` | `5` | The client has the ability to request particular batch size/duration. |
| waitTimeMs | `false` | `1000l` | The amount of time to wait (in milliseconds) if no data is available to pull from NiFi. | 

## Sink

The NiFi Sink Connector that delivers data to Apache NiFi using the NiFi Site-to-Site client. The sink requires a NiFiDataPacketBuilder which can create instances of NiFiDataPacket from the incoming data.

### Sink Configuration Options


| Name | Required | Default | Description |
|------|----------|---------|-------------|
| url | `true` | `null` | Specifies the URL of the remote NiFi instance. |
| portName | `true` | `null` | Specifies the name of the port to communicate with. Either the port name or the port identifier must be specified. |
| requestBatchCount | `false` | `5` | The client has the ability to request particular batch size/duration. |
| waitTimeMs | `false` | `1000l` | The amount of time to wait (in milliseconds) if no data is available to pull from NiFi. | 
