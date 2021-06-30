---
id: version-2.8.1-standalone
title: Set up a standalone Pulsar locally
sidebar_label: Run Pulsar locally
original_id: standalone
---

For local development and testing, you can run Pulsar in standalone mode on your machine. The standalone mode includes a Pulsar broker, the necessary ZooKeeper and BookKeeper components running inside of a single Java Virtual Machine (JVM) process.

> #### Pulsar in production? 
> If you're looking to run a full production Pulsar installation, see the [Deploying a Pulsar instance](deploy-bare-metal.md) guide.

## Install Pulsar standalone

This tutorial guides you through every step of the installation process.

### System requirements

Currently, Pulsar is available for 64-bit **macOS**, **Linux**, and **Windows**. To use Pulsar, you need to install 64-bit JRE/JDK 8 or later versions.

> #### Tip
> By default, Pulsar allocates 2G JVM heap memory to start. It can be changed in `conf/pulsar_env.sh` file under `PULSAR_MEM`. This is extra options passed into JVM. 

> **Note**
> 
> Broker is only supported on 64-bit JVM.

### Install Pulsar using binary release

To get started with Pulsar, download a binary tarball release in one of the following ways:

* download from the Apache mirror (<a href="pulsar:binary_release_url" download>Pulsar {{pulsar:version}} binary release</a>)

* download from the Pulsar [downloads page](pulsar:download_page_url)  
  
* download from the Pulsar [releases page](https://github.com/apache/pulsar/releases/latest)
  
* use [wget](https://www.gnu.org/software/wget):

  ```shell
  $ wget pulsar:binary_release_url
  ```

After you download the tarball, untar it and use the `cd` command to navigate to the resulting directory:

```bash
$ tar xvfz apache-pulsar-{{pulsar:version}}-bin.tar.gz
$ cd apache-pulsar-{{pulsar:version}}
```

#### What your package contains

The Pulsar binary package initially contains the following directories:

Directory | Contains
:---------|:--------
`bin` | Pulsar's command-line tools, such as [`pulsar`](reference-cli-tools.md#pulsar) and [`pulsar-admin`](https://pulsar.apache.org/tools/pulsar-admin/).
`conf` | Configuration files for Pulsar, including [broker configuration](reference-configuration.md#broker), [ZooKeeper configuration](reference-configuration.md#zookeeper), and more.
`examples` | A Java JAR file containing [Pulsar Functions](functions-overview.md) example.
`lib` | The [JAR](https://en.wikipedia.org/wiki/JAR_(file_format)) files used by Pulsar.
`licenses` | License files, in the`.txt` form, for various components of the Pulsar [codebase](https://github.com/apache/pulsar).

These directories are created once you begin running Pulsar.

Directory | Contains
:---------|:--------
`data` | The data storage directory used by ZooKeeper and BookKeeper.
`instances` | Artifacts created for [Pulsar Functions](functions-overview.md).
`logs` | Logs created by the installation.

> #### Tip
> If you want to use builtin connectors and tiered storage offloaders, you can install them according to the following instructions：
> 
> * [Install builtin connectors (optional)](#install-builtin-connectors-optional)
> * [Install tiered storage offloaders (optional)](#install-tiered-storage-offloaders-optional)
> 
> Otherwise, skip this step and perform the next step [Start Pulsar standalone](#start-pulsar-standalone). Pulsar can be successfully installed without installing bulitin connectors and tiered storage offloaders.

### Install builtin connectors (optional)

Since `2.1.0-incubating` release, Pulsar releases a separate binary distribution, containing all the `builtin` connectors.
To enable those `builtin` connectors, you can download the connectors tarball release in one of the following ways:

* download from the Apache mirror <a href="pulsar:connector_release_url" download>Pulsar IO Connectors {{pulsar:version}} release</a>

* download from the Pulsar [downloads page](pulsar:download_page_url)

* download from the Pulsar [releases page](https://github.com/apache/pulsar/releases/latest)

* use [wget](https://www.gnu.org/software/wget):

  ```shell
  $ wget pulsar:connector_release_url/{connector}-{{pulsar:version}}.nar
  ```

After you download the nar file, copy the file to the `connectors` directory in the pulsar directory. 
For example, if you download the `pulsar-io-aerospike-{{pulsar:version}}.nar` connector file, enter the following commands:

```bash
$ mkdir connectors
$ mv pulsar-io-aerospike-{{pulsar:version}}.nar connectors

$ ls connectors
pulsar-io-aerospike-{{pulsar:version}}.nar
...
```

> #### Note
>
> * If you are running Pulsar in a bare metal cluster, make sure `connectors` tarball is unzipped in every pulsar directory of the broker
> (or in every pulsar directory of function-worker if you are running a separate worker cluster for Pulsar Functions).
> 
> * If you are [running Pulsar in Docker](getting-started-docker.md) or deploying Pulsar using a docker image (e.g. [K8S](deploy-kubernetes.md) or [DCOS](deploy-dcos.md)),
> you can use the `apachepulsar/pulsar-all` image instead of the `apachepulsar/pulsar` image. `apachepulsar/pulsar-all` image has already bundled [all builtin connectors](io-overview.md#working-with-connectors).

### Install tiered storage offloaders (optional)

> #### Tip
>
> Since `2.2.0` release, Pulsar releases a separate binary distribution, containing the tiered storage offloaders.
> To enable tiered storage feature, follow the instructions below; otherwise skip this section.

To get started with [tiered storage offloaders](concepts-tiered-storage.md), you need to download the offloaders tarball release on every broker node in one of the following ways:

* download from the Apache mirror <a href="pulsar:offloader_release_url" download>Pulsar Tiered Storage Offloaders {{pulsar:version}} release</a>

* download from the Pulsar [downloads page](pulsar:download_page_url)

* download from the Pulsar [releases page](https://github.com/apache/pulsar/releases/latest)

* use [wget](https://www.gnu.org/software/wget):

  ```shell
  $ wget pulsar:offloader_release_url
  ```

After you download the tarball, untar the offloaders package and copy the offloaders as `offloaders`
in the pulsar directory:

```bash
$ tar xvfz apache-pulsar-offloaders-{{pulsar:version}}-bin.tar.gz

// you will find a directory named `apache-pulsar-offloaders-{{pulsar:version}}` in the pulsar directory
// then copy the offloaders

$ mv apache-pulsar-offloaders-{{pulsar:version}}/offloaders offloaders

$ ls offloaders
tiered-storage-jcloud-{{pulsar:version}}.nar
```

For more information on how to configure tiered storage, see [Tiered storage cookbook](cookbooks-tiered-storage.md).

> #### Note
>
> * If you are running Pulsar in a bare metal cluster, make sure that `offloaders` tarball is unzipped in every broker's pulsar directory.
> 
> * If you are [running Pulsar in Docker](getting-started-docker.md) or deploying Pulsar using a docker image (e.g. [K8S](deploy-kubernetes.md) or [DCOS](deploy-dcos.md)),
> you can use the `apachepulsar/pulsar-all` image instead of the `apachepulsar/pulsar` image. `apachepulsar/pulsar-all` image has already bundled tiered storage offloaders.

## Start Pulsar standalone

Once you have an up-to-date local copy of the release, you can start a local cluster using the [`pulsar`](reference-cli-tools.md#pulsar) command, which is stored in the `bin` directory, and specifying that you want to start Pulsar in standalone mode.

```bash
$ bin/pulsar standalone
```

If you have started Pulsar successfully, you will see `INFO`-level log messages like this:

```bash
2017-06-01 14:46:29,192 - INFO  - [main:WebSocketService@95] - Configuration Store cache started
2017-06-01 14:46:29,192 - INFO  - [main:AuthenticationService@61] - Authentication is disabled
2017-06-01 14:46:29,192 - INFO  - [main:WebSocketService@108] - Pulsar WebSocket Service started
```

> #### Tip
> 
> * The service is running on your terminal, which is under your direct control. If you need to run other commands, open a new terminal window.  
You can also run the service as a background process using the `pulsar-daemon start standalone` command. For more information, see [pulsar-daemon](https://pulsar.apache.org/docs/en/reference-cli-tools/#pulsar-daemon).
> 
> * By default, there is no encryption, authentication, or authorization configured. Apache Pulsar can be accessed from remote server without any authorization. Please do check [Security Overview](security-overview.md) document to secure your deployment.
>
> * When you start a local standalone cluster, a `public/default` [namespace](concepts-messaging.md#namespaces) is created automatically. The namespace is used for development purposes. All Pulsar topics are managed within namespaces. For more information, see [Topics](concepts-messaging.md#topics).

## Use Pulsar standalone

Pulsar provides a CLI tool called [`pulsar-client`](reference-cli-tools.md#pulsar-client). The pulsar-client tool enables you to consume and produce messages to a Pulsar topic in a running cluster. 

### Consume a message

The following command consumes a message with the subscription name `first-subscription` to the `my-topic` topic:

```bash
$ bin/pulsar-client consume my-topic -s "first-subscription"
```

If the message has been successfully consumed, you will see a confirmation like the following in the `pulsar-client` logs:

```
09:56:55.566 [pulsar-client-io-1-1] INFO  org.apache.pulsar.client.impl.MultiTopicsConsumerImpl - [TopicsConsumerFakeTopicNamee2df9] [first-subscription] Success subscribe new topic my-topic in topics consumer, partitions: 4, allTopicPartitionsNumber: 4
```

> #### Tip
>  
> As you have noticed that we do not explicitly create the `my-topic` topic, to which we consume the message. When you consume a message to a topic that does not yet exist, Pulsar creates that topic for you automatically. Producing a message to a topic that does not exist will automatically create that topic for you as well.

### Produce a message

The following command produces a message saying `hello-pulsar` to the `my-topic` topic:

```bash
$ bin/pulsar-client produce my-topic --messages "hello-pulsar"
```

If the message has been successfully published to the topic, you will see a confirmation like the following in the `pulsar-client` logs:

```
13:09:39.356 [main] INFO  org.apache.pulsar.client.cli.PulsarClientTool - 1 messages successfully produced
```

## Stop Pulsar standalone

Press `Ctrl+C` to stop a local standalone Pulsar.

> #### Tip
> 
> If the service runs as a background process using the `pulsar-daemon start standalone` command, then use the `pulsar-daemon stop standalone`  command to stop the service.
> 
> For more information, see [pulsar-daemon](https://pulsar.apache.org/docs/en/reference-cli-tools/#pulsar-daemon).
