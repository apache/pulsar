---
title: The Pulsar proxy
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

The [Pulsar proxy](../../getting-started/ConceptsAndArchitecture#pulsar-proxy) is an optional gateway that you can run over the {% popover brokers %} in a Pulsar {% popover cluster %}. We recommend running a Pulsar proxy in cases when direction connections between clients and Pulsar brokers are either infeasible, undesirable, or both, for example when running Pulsar in a cloud environment or on [Kubernetes](https://kubernetes.io) or an analogous platform.

## Running the proxy

In order to run the Pulsar proxy, you need to have both a local [ZooKeeper](https://zookeeper.apache.org) and {% popover configuration store %} quorum set up for use by your Pulsar cluster. For instructions, see [this document](../../deployment/cluster). Once you have ZooKeeper set up and have connection strings for both ZooKeeper quorums, you can use the [`proxy`](../../reference/CliTools#pulsar-proxy) command of the [`pulsar`](../../reference/CliTools#pulsar) CLI tool to start up the proxy (preferably on its own machine or in its own VM):

To start the proxy:

```bash
$ cd /path/to/pulsar/directory
$ bin/pulsar proxy \
  --zookeeper-servers zk-0,zk-1,zk-2 \
  --configuration-store-servers zk-0,zk-1,zk-2
```

{% include admonition.html type="success" content="You can run as many instances of the Pulsar proxy in a cluster as you would like." %}

## Stopping the proxy

The Pulsar proxy runs by default in the foreground. To stop the proxy, simply stop the process in which it's running.

## Proxy frontends

We recommend running the Pulsar proxy behind some kind of load-distributing frontend, such as an [HAProxy](https://www.digitalocean.com/community/tutorials/an-introduction-to-haproxy-and-load-balancing-concepts) load balancer.

## Using Pulsar clients with the proxy

Once your Pulsar proxy is up and running, preferably behind a load-distributing [frontend](#proxy-frontends), clients can connect to the proxy via whichever address is used by the frontend. If the address were the DNS address `pulsar.cluster.default`, for example, then the connection URL for clients would be `pulsar://pulsar.cluster.default:6650`.

## Proxy configuration

The Pulsar proxy can be configured using the [`proxy.conf`](../../reference/Configuration#proxy) configuration file. The following parameters are available in that file:

{% include config.html id="proxy" %}