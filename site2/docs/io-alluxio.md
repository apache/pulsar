---
id: io-alluxio
title: alluxio Connector
sidebar_label: alluxio Connector
---

## Sink

The alluxio Sink Connector is used to pull messages from Pulsar topics and persist the messages
to an alluxio directory.

## Sink Configuration Options

| Name | Default | Required | Description |
|------|---------|----------|-------------|
| `alluxioMasterHost` | `null` | `true` | The hostname of Alluxio master. |
| `alluxioMasterPort` | `19998` | `true` | The port that Alluxio master node runs on. |
| `alluxioDir` | `null` | `true` | The alluxio directory from which files should be read from or written to. |
| `securityLoginUser` | `null` | `false` | When alluxio.security.authentication.type is set to SIMPLE or CUSTOM, user application uses this property to indicate the user requesting Alluxio service. If it is not set explicitly, the OS login user will be used. |
| `filePrefix` | `null` | `false` | The prefix of the files to create in the Alluxio directory (e.g. a value of 'TopicA' will result in files named topicA-, topicA-, etc being produced). |
| `fileExtension` | `null` | `false` | The extension to add to the files written to Alluxio (e.g. '.txt'). |
| `lineSeparator` | `null` | `false` | The character used to separate records in a text file. If no value is provided then the content from all of the records will be concatenated together in one continuous byte array. |
| `rotationRecords` | `10000` | `false` | The number records of alluxio file rotation. |
| `rotationInterval` | `-1` | `false` | The interval (in milliseconds) to rotate a alluxio file. |
| `writeType` | `MUST_CACHE` | `false` | Default write type when creating Alluxio files. Valid options are `MUST_CACHE` (write will only go to Alluxio and must be stored in Alluxio), `CACHE_THROUGH` (try to cache, write to UnderFS synchronously), `THROUGH` (no cache, write to UnderFS synchronously). |
