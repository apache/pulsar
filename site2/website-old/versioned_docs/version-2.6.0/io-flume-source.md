---
id: version-2.6.0-io-flume-source
title: Flume source connector
sidebar_label: Flume source connector
original_id: io-flume-source
---

The Flume source connector pulls messages from logs to Pulsar topics.

## Configuration

The configuration of the Flume source connector has the following properties.

### Property

| Name | Type|Required | Default | Description 
|------|----------|----------|---------|-------------|
`name`|String|true|"" (empty string)|The name of the agent.
`confFile`|String|true|"" (empty string)|The configuration file.
`noReloadConf`|Boolean|false|false|Whether to reload configuration file if changed.
`zkConnString`|String|true|"" (empty string)|The ZooKeeper connection.
`zkBasePath`|String|true|"" (empty string)|The base path in ZooKeeper for agent configuration.

### Example

Before using the Flume source connector, you need to create a configuration file through one of the following methods.

> For more information about the `source.conf` in the example below, see [here](https://github.com/apache/pulsar/blob/master/pulsar-io/flume/src/main/resources/flume/source.conf).

* JSON 

    ```json
    {
        "name": "a1",
        "confFile": "source.conf",
        "noReloadConf": "false",
        "zkConnString": "",
        "zkBasePath": ""
    }
    ```

* YAML

    ```yaml
    configs:
        name: a1
        confFile: source.conf
        noReloadConf: false
        zkConnString: ""
        zkBasePath: ""
    ```

