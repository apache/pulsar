---
id: io-alluxio
title: Alluxio sink connector
sidebar_label: Alluxio sink connector
---

## Sink

The Alluxio sink connector pulls messages from Pulsar topics and persists the messages to an Alluxio directory.

## Configuration

The configuration of the Alluxio sink connector has the following properties.

### Property

| Name | Type|Required | Default | Description 
|------|----------|----------|---------|-------------|
| `alluxioMasterHost` | String | true | "" (empty string) | The hostname of Alluxio master. |
| `alluxioMasterPort` | int | true | 19998 | The port that Alluxio master node runs on. |
| `alluxioDir` | String | true | "" (empty string) | The Alluxio directory from which files should be read from or written to. |
| `securityLoginUser` | String | false | "" (empty string) | When `alluxio.security.authentication.type` is set to `SIMPLE` or `CUSTOM`, user application uses this property to indicate the user requesting Alluxio service. If it is not set explicitly, the OS login user is used. |
| `filePrefix` | String | false | "" (empty string) | The prefix of the files to create in the Alluxio directory (e.g. a value of 'TopicA' results in files named topicA-, topicA-, etc being produced). |
| `fileExtension` | String | false | "" (empty string) | The extension to add to the files written to Alluxio (e.g. '.txt'). |
| `lineSeparator` | String | false | "" (empty string) | The character used to separate records in a text file. If no value is provided, then the content from all of the records is concatenated together in one continuous byte array. |
| `rotationRecords` | long | false | 10000 | The number records of Alluxio file rotation. |
| `rotationInterval` | long | false | -1 | The interval to rotate a Alluxio file (in milliseconds). |
| `schemaEnable` | boolean | false | false | Sets whether the Sink has to take into account the Schema or if it should simply copy the raw message to Alluxio. |
| `writeType` | String | false | `MUST_CACHE` | Default write type when creating Alluxio files. Valid options are `MUST_CACHE` (write only goes to Alluxio and must be stored in Alluxio), `CACHE_THROUGH` (try to cache, write to UnderFS synchronously), `THROUGH` (no cache, write to UnderFS synchronously). |

### Example

Before using the Alluxio sink connector, you need to create a configuration file in the path you will start Pulsar service (i.e. `PULSAR_HOME`) through one of the following methods.

* JSON

    ```json
    {
        "alluxioMasterHost": "localhost",
        "alluxioMasterPort": "19998",
        "alluxioDir": "pulsar",
        "filePrefix": "TopicA",
        "fileExtension": ".txt",
        "lineSeparator": "\n",
        "rotationRecords": "100",
        "rotationInterval": "-1"
    }
    ```

* YAML

    ```yaml
    configs:
        alluxioMasterHost: "localhost"
        alluxioMasterPort: "19998"
        alluxioDir: "pulsar"
        filePrefix: "TopicA"
        fileExtension: ".txt"
        lineSeparator: "\n"
        rotationRecords: 100
        rotationInterval: "-1"
    ```