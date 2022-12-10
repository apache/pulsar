---
id: reference-cli-tools
title: Pulsar command-line tools
sidebar_label: "Pulsar CLI tools"
---

Pulsar offers several command-line tools that you can use for managing Pulsar installations, performance testing, using command-line producers and consumers, and more.

* `pulsar-admin`
* `pulsar`
* `pulsar-client`
* `pulsar-daemon`
* `pulsar-perf`
* `pulsar-shell`
* `bookkeeper`

:::tip

For the latest and complete information about command-line tools, including commands, flags, descriptions, and more information, see [Pulsar Reference](https://pulsar.apache.org/reference).

:::

All Pulsar command-line tools can be run from the `bin` directory of your [installed Pulsar package](getting-started-standalone.md). 

You can get help for any CLI tool, command, or subcommand using the `--help` flag, or `-h` for short. Here's an example:

```shell
bin/pulsar broker --help
```