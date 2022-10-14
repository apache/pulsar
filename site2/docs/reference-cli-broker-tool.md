---
id: reference-cli-broker-tool
title: Pulsar CLI tools - broker-tool
sidebar_label: "cli-broker-tool"
---

## `broker-tool`

The `broker- tool` is used for operations on a specific broker.

Usage

```bash
broker-tool command
```

Example

Two ways to get more information about a command as below:

```bash
broker-tool help command
broker-tool command --help
```

### `load-report`

Collect the load report of a specific broker. 
The command is run on a broker, and used for troubleshooting why broker can’t collect right load report.

Options

|Flag|Description|Default|
|---|---|---|
|`-i`, `--interval`| Interval to collect load report, in milliseconds ||
|`-h`, `--help`| Display help information ||
