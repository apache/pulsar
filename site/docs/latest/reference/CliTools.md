---
title: Pulsar command-line tools
tags:
- admin
- cli
- client
- daemon
- perf
- bookkeeper
---

Pulsar offers several command-line tools that you can use for managing Pulsar installations, performance testing, using command-line {% popover producers %} and {% popover consumers %}, and more.

All Pulsar command-line tools can be run from the `bin` directory of your [installed Pulsar package](../../getting-started/LocalCluster#installing). The following tools are currently documented:

* [`pulsar`](#pulsar)
* [`pulsar-admin`](#pulsar-admin)
* [`pulsar-client`](#pulsar-client)
* [`pulsar-daemon`](#pulsar-daemon)
* [`pulsar-perf`](#pulsar-perf)
* [`bookkeeper`](#bookkeeper)

{% include admonition.html type='success' title='Getting help' content="
You can get help for any CLI tool, command, or subcommand using the `--help` flag, or `-h` for short. Here's an example:

```shell
$ bin/pulsar-admin clusters --help
```
" %}

{% include cli.html tool="pulsar" %}

{% include cli.html tool="pulsar-admin" %}

{% include cli.html tool="pulsar-client" %}

{% include cli.html tool="pulsar-daemon" %}

{% include cli.html tool="pulsar-perf" %}

{% include cli.html tool="bookkeeper" %}
