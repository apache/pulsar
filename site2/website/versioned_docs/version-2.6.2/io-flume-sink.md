---
id: version-2.6.2-io-flume-sink
title: Flume sink connector
sidebar_label: Flume sink connector
original_id: io-flume-sink
---

The Flume sink connector pulls messages from Pulsar topics to logs.

## Configuration

The configuration of the Flume sink connector has the following properties.

### Property

| Name | Type|Required | Default | Description 
|------|----------|----------|---------|-------------|
`name`|String|true|"" (empty string)|The name of the agent.
`confFile`|String|true|"" (empty string)|The configuration file.
`noReloadConf`|Boolean|false|false|Whether to reload configuration file if changed.
`zkConnString`|String|true|"" (empty string)|The ZooKeeper connection.
`zkBasePath`|String|true|"" (empty string)|The base path in ZooKeeper for agent configuration.

### Example

Before using the Flume sink connector, you need to create a configuration file through one of the following methods.

> For more information about the `sink.conf` in the example below, see [here](https://github.com/apache/pulsar/blob/master/pulsar-io/flume/src/main/resources/flume/sink.conf).

* JSON 

    ```json
    {
        "name": "a1",
        "confFile": "sink.conf",
        "noReloadConf": "false",
        "zkConnString": "",
        "zkBasePath": ""
    }
    ```

* YAML

    ```yaml
    configs:
        name: a1
        confFile: sink.conf
        noReloadConf: false
        zkConnString: ""
        zkBasePath: ""
    ```

