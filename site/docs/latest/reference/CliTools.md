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

<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

Pulsar offers several command-line tools that you can use for managing Pulsar installations, performance testing, using command-line {% popover producers %} and {% popover consumers %}, and more.

All Pulsar command-line tools can be run from the `bin` directory of your [installed Pulsar package](../../getting-started/LocalCluster#installing-pulsar). The following tools are currently documented:

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
