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

{% capture binary_release_url %}http://www.apache.org/dyn/closer.cgi/incubator/pulsar/pulsar-{{ site.current_version }}/apache-pulsar-{{ site.current_version }}-bin.tar.gz{% endcapture %}

## System requirements

Pulsar is currently available for **MacOS** and **Linux**. In order to use Pulsar, you'll need to install [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html).

## Installing Pulsar

To get started running Pulsar, download a binary tarball release in one of the following ways:

* by clicking the link below and downloading the release from an Apache mirror:

  * <a href="{{ binary_release_url }}" download>Pulsar {{ site.current_version }} binary release</a>

* from the Pulsar [downloads page](/download)
* from the Pulsar [releases page](https://github.com/apache/incubator-pulsar/releases/latest)
* using [wget](https://www.gnu.org/software/wget):

  ```shell
  $ wget 'https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=incubator/pulsar/pulsar-{{ site.current_version }}/apache-pulsar-{{ site.current_version }}-bin.tar.gz' -O apache-pulsar-{{ site.current_version }}-bin.tar.gz
  ```

Once the tarball is downloaded, untar it and `cd` into the resulting directory:

```bash
$ tar xvfz apache-pulsar-{{ site.current_version }}-bin.tar.gz
$ cd apache-pulsar-{{ site.current_version }}
```

## What your package contains

The Pulsar binary package initially contains the following directories:

Directory | Contains
:---------|:--------
`bin` | Pulsar's [command-line tools](../../reference/CliTools), such as [`pulsar`](../../reference/CliTools#pulsar) and [`pulsar-admin`](../../reference/CliTools#pulsar-admin)
`conf` | Configuration files for Pulsar, including for [broker configuration](../../reference/Configuration#broker), [ZooKeeper configuration](../../reference/Configuration#zookeeper), and more
`examples` | A Java JAR file containing example [Pulsar Functions](../../functions/overview)
`lib` | The [JAR](https://en.wikipedia.org/wiki/JAR_(file_format)) files used by Pulsar
`licenses` | License files, in `.txt` form, for various components of the Pulsar [codebase](../../project/Codebase)

These directories will be created once you begin running Pulsar:

Directory | Contains
:---------|:--------
`data` | The data storage directory used by {% popover ZooKeeper %} and {% popover BookKeeper %}
`instances` | Artifacts created for [Pulsar Functions](../../functions/overview)
`logs` | Logs created by the installation
