---
title: Deploying a Pulsar cluster on bare metal
tags: [admin, deployment, cluster, bare metal]
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

<!-- Convenience variables to be used for download links -->
{% capture binary_release_url %}http://www.apache.org/dyn/closer.cgi/incubator/pulsar/pulsar-{{ site.current_version }}/apache-pulsar-{{ site.current_version }}-bin.tar.gz{% endcapture %}

You can deploy Pulsar either as a single [cluster](#deploying-a-single-pulsar-cluster) or as a multi-cluster [instance](#deploying-a-multi-cluster-pulsar-instance).

{% include admonition.html type="info"
  content="Single-cluster Pulsar installations should be sufficient for all but the most ambitious use cases. If you're interested in experimenting with Pulsar or using it in a startup or on a single team, we recommend opting for a single cluster." %}

## Deploying a single Pulsar cluster

Deploying a Pulsar {% popover cluster %} involves deploying the following (in order):

* [ZooKeeper](#deploying-a-zookeeper-cluster)
* [BookKeeper](#deploying-a-bookkeeper-cluster)
* One or more Pulsar [brokers](#deploying-pulsar-brokers)

### System requirements

To run Pulsar on bare metal, each machine in your cluster will need to have [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/index.html) or higher installed.

### Installing the Pulsar binary package

{% include admonition.html type="info"
   content="You'll need to install the Pulsar binary package on *each machine in the cluster*, including machines running [ZooKeeper](#deploying-a-zookeeper-cluster) and [BookKeeper](#deploying-a-bookkeeper-cluster)." %}

To get started deploying a Pulsar cluster on bare metal, you'll need to download a binary tarball release in one of the following ways:

* By clicking on the link directly below, which will automatically trigger a download:
  * <a href="{{ binary_release_url }}" download>Pulsar {{ site.current_version }} binary release</a>
* From the Pulsar [downloads page](http://pulsar.incubator.apache.org/download)
* From the Pulsar [releases page](https://github.com/apache/incubator-pulsar/releases/latest) on [GitHub](https://github.com)
* Using [wget](https://www.gnu.org/software/wget):

  ```bash
  $ wget http://archive.apache.org/dist/incubator/pulsar/pulsar-{{ site.current_version }}/apache-pulsar-{{ site.current_version }}-bin.tar.gz
  ```

Once you've downloaded the tarball, untar it and `cd` into the resulting directory:

```bash
$ tar xvzf apache-pulsar-{{ site.current_version }}-bin.tar.gz
$ cd apache-pulsar-{{ site.current_version }}
```

The untarred directory contains the following subdirectories:

Directory | Contains
:---------|:--------
`bin` | Pulsar's [command-line tools](../../reference/CliTools), such as [`pulsar`](../../reference/CliTools#pulsar) and [`pulsar-admin`](../../reference/CliTools#pulsar-admin)
`conf` | Configuration files for Pulsar, including for [broker configuration](../../reference/Configuration#broker), [ZooKeeper configuration](../../reference/Configuration#zookeeper), and more
`data` | The data storage directory used by {% popover ZooKeeper %} and {% popover BookKeeper %}.
`lib` | The [JAR](https://en.wikipedia.org/wiki/JAR_(file_format)) files used by Pulsar.
`logs` | Logs created by the installation.

### Deploying a ZooKeeper cluster

[ZooKeeper](https://zookeeper.apache.org) manages a variety of essential coordination- and configuration-related tasks for Pulsar. To deploy a Pulsar cluster you'll need to deploy ZooKeeper first (before all other components).

### Deploying a BookKeeper cluster

[BookKeeper](https://bookkeeper.apache.org)