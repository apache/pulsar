---
id: io-flume-source
title: Flume source connector
sidebar_label: Flume source connector
---

The Flume source connector pulls messages from logs to Pulsar topics.

## Configuration

The configuration of Flume source connector has the following parameters.

### Parameter

| Name | Type|Required | Default | Description 
|------|----------|----------|---------|-------------|
`name`|String|true|"" (empty string)|Name of the agent
`confFile`|String|true|"" (empty string)|Configuration file
`noReloadConf`|Boolean|false|false|Whether to reload configuration file if changed
`zkConnString`|String|true|"" (empty string)|ZooKeeper connection 
`zkBasePath`|String|true|"" (empty string)|Base path in ZooKeeper for agent configuration
