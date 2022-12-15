---
id: getting-started-standalone
title: Set up a standalone Pulsar locally
sidebar_label: "Run Pulsar locally"
original_id: getting-started-standalone
---

For local development and testing, you can run Pulsar in standalone mode on your machine. The standalone mode includes a Pulsar broker, the necessary [RocksDB](http://rocksdb.org/) and BookKeeper components running inside of a single Java Virtual Machine (JVM) process.

> **Pulsar in production?**  
> If you're looking to run a full production Pulsar installation, see the [Deploying a Pulsar instance](deploy-bare-metal.md) guide.

## Install Pulsar standalone

This tutorial guides you through every step of installing Pulsar locally.

### System requirements

Currently, Pulsar is available for 64-bit **macOS**, **Linux**, and **Windows**. To use Pulsar, you need to install 64-bit JRE/JDK 8 or later versions

:::tip

By default, Pulsar allocates 2G JVM heap memory to start. It can be changed in `conf/pulsar_env.sh` file under `PULSAR_MEM`. This is an extra option passed into JVM. 

:::

:::note

Broker is only supported on 64-bit JVM.

:::

#### Install JDK on M1
In the current version, Pulsar uses a BookKeeper version which in turn uses RocksDB. RocksDB is compiled to work on x86 architecture and not ARM. Therefore, Pulsar can only work with x86 JDK. This is planned to be fixed in future versions of Pulsar.

One of the ways to easily install an x86 JDK is to use [SDKMan](http://sdkman.io) as outlined in the following steps:

1. Install [SDKMan](http://sdkman.io).

Follow the instructions on the SDKMan website.

2. Turn on Rosetta2 compatibility for SDKMan by editing `~/.sdkman/etc/config` and changing the following property from `false` to `true`.

```properties
sdkman_rosetta2_compatible=true
```

3. Close the current shell / terminal window and open a new one.
4. Make sure you don't have any previously installed JVM of the same version by listing existing installed versions.

```shell
sdk list java|grep installed
```

Example output:

```text
               | >>> | 17.0.3.6.1   | amzn    | installed  | 17.0.3.6.1-amzn
```

If you have any Java 17 version installed, uninstall it.

```shell
sdk uinstall java 17.0.3.6.1
```

5. Install any Java versions greater than Java 8.

```shell
sdk install java 17.0.3.6.1-amzn
```

### Install Pulsar using binary release

To get started with Pulsar, download a binary tarball release in one of the following ways:

* download from the Apache mirror (<a href="pulsar:binary_release_url" download>Pulsar @pulsar:version@ binary release</a>)

* download from the Pulsar [downloads page](pulsar:download_page_url)  
  
* download from the Pulsar [releases page](https://github.com/apache/pulsar/releases/latest)
  
* use [wget](https://www.gnu.org/software/wget):

  ```shell
  wget pulsar:binary_release_url
  ```

After you download the tarball, untar it and use the `cd` command to navigate to the resulting directory:

```bash
tar xvfz apache-pulsar-@pulsar:version@-bin.tar.gz
cd apache-pulsar-@pulsar:version@
```

#### What your package contains

The Pulsar binary package initially contains the following directories:

Directory | Contains
:---------|:--------
`bin` | Pulsar's command-line tools, such as [`pulsar`](reference-cli-tools.md#pulsar) and [`pulsar-admin`](/tools/pulsar-admin/).
`conf` | Configuration files for Pulsar, including [broker configuration](reference-configuration.md#broker) and more.<br />**Note:** Pulsar standalone uses RocksDB as the local metadata store and its configuration file path [`metadataStoreConfigPath`](reference-configuration.md) is configurable in the `standalone.conf` file. For more information about the configurations of RocksDB, see [here](https://github.com/facebook/rocksdb/blob/main/examples/rocksdb_option_file_example.ini) and related [documentation](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide).
`examples` | A Java JAR file containing [Pulsar Functions](functions-overview.md) example.
`instances` | Artifacts created for [Pulsar Functions](functions-overview.md).
`lib` | The [JAR](https://en.wikipedia.org/wiki/JAR_(file_format)) files used by Pulsar.
`licenses` | License files, in the`.txt` form, for various components of the Pulsar [codebase](https://github.com/apache/pulsar).

These directories are created once you begin running Pulsar.

Directory | Contains
:---------|:--------
`data` | The data storage directory used by RocksDB and BookKeeper.
`logs` | Logs created by the installation.

:::tip

If you want to use built-in connectors and tiered storage offloaders, you can install them according to the following instructions：
* [Install built-in connectors (optional)](#install-builtin-connectors-optional)
* [Install tiered storage offloaders (optional)](#install-tiered-storage-offloaders-optional)
Otherwise, skip this step and perform the next step [Start Pulsar standalone](#start-pulsar-standalone). Pulsar can be successfully installed without installing built-in connectors and tiered storage offloaders.

:::

### Install builtin connectors (optional)

Since `2.1.0-incubating` release, Pulsar releases a separate binary distribution, containing all the `built-in` connectors.
To enable those `builtin` connectors, you can download the connectors tarball release in one of the following ways:

* download from the Apache mirror <a href="pulsar:connector_release_url" download>Pulsar IO Connectors @pulsar:version@ release</a>

* download from the Pulsar [downloads page](pulsar:download_page_url)

* download from the Pulsar [releases page](https://github.com/apache/pulsar/releases/latest)

* use [wget](https://www.gnu.org/software/wget):

  ```shell
  wget pulsar:connector_release_url/{connector}-@pulsar:version@.nar
  ```

After you download the NAR file, copy the file to the `connectors` directory in the pulsar directory. 
For example, if you download the `pulsar-io-aerospike-@pulsar:version@.nar` connector file, enter the following commands:

```bash
mkdir connectors
mv pulsar-io-aerospike-@pulsar:version@.nar connectors

ls connectors
pulsar-io-aerospike-@pulsar:version@.nar
...
```

:::note

* If you are running Pulsar in a bare metal cluster, make sure `connectors` tarball is unzipped in every pulsar directory of the broker (or in every pulsar directory of function-worker if you are running a separate worker cluster for Pulsar Functions).
* If you are [running Pulsar in Docker](getting-started-docker.md) or deploying Pulsar using a docker image (e.g. [K8S](deploy-kubernetes.md) or [DC/OS](https://dcos.io/), you can use the `apachepulsar/pulsar-all` image instead of the `apachepulsar/pulsar` image. `apachepulsar/pulsar-all` image has already bundled [all built-in connectors](io-overview.md#working-with-connectors).

:::

### Install tiered storage offloaders (optional)

:::tip

- Since `2.2.0` release, Pulsar releases a separate binary distribution, containing the tiered storage offloaders.
- To enable the tiered storage feature, follow the instructions below; otherwise skip this section.

:::

To get started with [tiered storage offloaders](concepts-tiered-storage.md), you need to download the offloaders tarball release on every broker node in one of the following ways:

* download from the Apache mirror <a href="pulsar:offloader_release_url" download>Pulsar Tiered Storage Offloaders @pulsar:version@ release</a>

* download from the Pulsar [downloads page](pulsar:download_page_url)

* download from the Pulsar [releases page](https://github.com/apache/pulsar/releases/latest)

* use [wget](https://www.gnu.org/software/wget):

  ```shell
  wget pulsar:offloader_release_url
  ```

After you download the tarball, untar the offloaders package and copy the offloaders as `offloaders`
in the pulsar directory:

```bash
tar xvfz apache-pulsar-offloaders-@pulsar:version@-bin.tar.gz

// you will find a directory named `apache-pulsar-offloaders-@pulsar:version@` in the pulsar directory
// then copy the offloaders

mv apache-pulsar-offloaders-@pulsar:version@/offloaders offloaders

ls offloaders
tiered-storage-jcloud-@pulsar:version@.nar

```

For more information on how to configure tiered storage, see [Tiered storage cookbook](cookbooks-tiered-storage.md).

:::note

* If you are running Pulsar in a bare metal cluster, make sure that `offloaders` tarball is unzipped in every broker's pulsar directory.
* If you are [running Pulsar in Docker](getting-started-docker.md) or deploying Pulsar using a docker image (e.g. [K8S](deploy-kubernetes.md) or DC/OS), you can use the `apachepulsar/pulsar-all` image instead of the `apachepulsar/pulsar` image. `apachepulsar/pulsar-all` image has already bundled tiered storage offloaders.

:::

## Start Pulsar standalone

Once you have an up-to-date local copy of the release, you can start a local cluster using the [`pulsar`](reference-cli-tools.md#pulsar) command, which is stored in the `bin` directory, and specifying that you want to start Pulsar in standalone mode.

```bash
bin/pulsar standalone
```

If you have started Pulsar successfully, you will see `INFO`-level log messages like this:

```bash
21:59:29.327 [DLM-/stream/storage-OrderedScheduler-3-0] INFO  org.apache.bookkeeper.stream.storage.impl.sc.StorageContainerImpl - Successfully started storage container (0).
21:59:34.576 [main] INFO  org.apache.pulsar.broker.authentication.AuthenticationService - Authentication is disabled
21:59:34.576 [main] INFO  org.apache.pulsar.websocket.WebSocketService - Pulsar WebSocket Service started
```

:::tip

* The service is running on your terminal, which is under your direct control. If you need to run other commands, open a new terminal window.  

:::

You can also run the service as a background process using the `bin/pulsar-daemon start standalone` command. For more information, see [pulsar-daemon](reference-cli-tools.md#pulsar-daemon).
> 
> * By default, there is no encryption, authentication, or authorization configured. Apache Pulsar can be accessed from remote server without any authorization. Please do check [Security Overview](security-overview.md) document to secure your deployment.
>
> * When you start a local standalone cluster, a `public/default` [namespace](concepts-messaging.md#namespaces) is created automatically. The namespace is used for development purposes. All Pulsar topics are managed within namespaces. For more information, see [Topics](concepts-messaging.md#topics).

## Use Pulsar standalone

Pulsar provides a CLI tool called [`pulsar-client`](reference-cli-tools.md#pulsar-client). The pulsar-client tool enables you to consume and produce messages to a Pulsar topic in a running cluster. 

### Consume a message

The following command consumes a message with the subscription name `first-subscription` to the `my-topic` topic:

```bash
bin/pulsar-client consume my-topic -s "first-subscription"
```

If the message has been successfully consumed, you will see a confirmation like the following in the `pulsar-client` logs:

```
22:17:16.781 [main] INFO  org.apache.pulsar.client.cli.PulsarClientTool - 1 messages successfully consumed
```

:::tip

As you have noticed that we do not explicitly create the `my-topic` topic, from which we consume the message. When you consume a message from a topic that does not yet exist, Pulsar creates that topic for you automatically. Producing a message to a topic that does not exist will automatically create that topic for you as well.

:::

### Produce a message

The following command produces a message saying `hello-pulsar` to the `my-topic` topic:

```bash
bin/pulsar-client produce my-topic --messages "hello-pulsar"
```

If the message has been successfully published to the topic, you will see a confirmation like the following in the `pulsar-client` logs:

```
22:21:08.693 [main] INFO  org.apache.pulsar.client.cli.PulsarClientTool - 1 messages successfully produced
```

## Stop Pulsar standalone

Press `Ctrl+C` to stop a local standalone Pulsar.

:::tip

If the service runs as a background process using the `bin/pulsar-daemon start standalone` command, then use the `bin/pulsar-daemon stop standalone` command to stop the service.
For more information, see [pulsar-daemon](reference-cli-tools.md#pulsar-daemon).

:::

