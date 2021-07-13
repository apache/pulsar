---
id: version-2.7.0-tiered-storage-filesystem
title: Use filesystem offloader with Pulsar
sidebar_label: Filesystem offloader
original_id: tiered-storage-filesystem
---

This chapter guides you through every step of installing and configuring the filesystem offloader and using it with Pulsar.

## Installation

Follow the steps below to install the filesystem offloader.

### Prerequisite

- Pulsar: 2.4.2 or later versions

- Hadoop: 3.x.x

### Step

This example uses Pulsar 2.5.1.

1. Download the Pulsar tarball using one of the following ways:

   * Download from the [Apache mirror](https://archive.apache.org/dist/pulsar/pulsar-2.5.1/apache-pulsar-2.5.1-bin.tar.gz)

   * Download from the Pulsar [download page](https://pulsar.apache.org/download)

   * Use [wget](https://www.gnu.org/software/wget)

     ```shell
     wget https://archive.apache.org/dist/pulsar/pulsar-2.5.1/apache-pulsar-2.5.1-bin.tar.gz
     ```

2. Download and untar the Pulsar offloaders package. 

    ```bash
    wget https://downloads.apache.org/pulsar/pulsar-2.5.1/apache-pulsar-offloaders-2.5.1-bin.tar.gz

    tar xvfz apache-pulsar-offloaders-2.5.1-bin.tar.gz
    ```

    > #### Note
    >
    > * If you are running Pulsar in a bare metal cluster, make sure that `offloaders` tarball is unzipped in every broker's Pulsar directory.
    > 
    > * If you are running Pulsar in Docker or deploying Pulsar using a Docker image (such as K8S and DCOS), you can use the `apachepulsar/pulsar-all` image instead of the `apachepulsar/pulsar` image. `apachepulsar/pulsar-all` image has already bundled tiered storage offloaders.

3. Copy the Pulsar offloaders as `offloaders` in the Pulsar directory.

    ```
    mv apache-pulsar-offloaders-2.5.1/offloaders apache-pulsar-2.5.1/offloaders

    ls offloaders
    ```

    **Output**

    ```
    tiered-storage-file-system-2.5.1.nar
    tiered-storage-jcloud-2.5.1.nar
    ```

    > #### Note
    >
    > * If you are running Pulsar in a bare metal cluster, make sure that `offloaders` tarball is unzipped in every broker's Pulsar directory.
    > 
    > * If you are running Pulsar in Docker or deploying Pulsar using a Docker image (such as K8s and DCOS), you can use the `apachepulsar/pulsar-all` image instead of the `apachepulsar/pulsar` image. `apachepulsar/pulsar-all` image has already bundled tiered storage offloaders.

## Configuration

> #### Note
> 
> Before offloading data from BookKeeper to filesystem, you need to configure some properties of the filesystem offloader driver. 

Besides, you can also configure the filesystem offloader to run it automatically or trigger it manually.

### Configure filesystem offloader driver

You can configure filesystem offloader driver in the configuration file `broker.conf` or `standalone.conf`.

- **Required** configurations are as below.
  
    Required configuration | Description | Example value
    |---|---|---
    `managedLedgerOffloadDriver` | Offloader driver name, which is case-insensitive. | filesystem
    `fileSystemURI` | Connection address | hdfs://127.0.0.1:9000
    `fileSystemProfilePath` | Hadoop profile path | ../conf/filesystem_offload_core_site.xml

- **Optional** configurations are as below.

    Optional configuration| Description | Example value
    |---|---|---
    `managedLedgerMinLedgerRolloverTimeMinutes`|Minimum time between ledger rollover for a topic<br><br>**Note**: it is not recommended that you set this configuration in the production environment.|2
    `managedLedgerMaxEntriesPerLedger`|Maximum number of entries to append to a ledger before triggering a rollover.<br><br>**Note**: it is not recommended that you set this configuration in the production environment.|5000

#### Offloader driver (required)

Offloader driver name, which is case-insensitive.

This example sets the offloader driver name as _filesystem_.

```conf
managedLedgerOffloadDriver=filesystem
```

#### Connection address (required)

Connection address is the URI to access the default Hadoop distributed file system. 

##### Example

This example sets the connection address as _hdfs://127.0.0.1:9000_.

```conf
fileSystemURI=hdfs://127.0.0.1:9000
```

#### Hadoop profile path (required)

The configuration file is stored in the Hadoop profile path. It contains various settings for Hadoop performance tuning.

##### Example

This example sets the Hadoop profile path as _../conf/filesystem_offload_core_site.xml_.

```conf
fileSystemProfilePath=../conf/filesystem_offload_core_site.xml
```

You can set the following configurations in the _filesystem_offload_core_site.xml_ file.

```
<property>
    <name>fs.defaultFS</name>
    <value></value>
</property>

<property>
    <name>hadoop.tmp.dir</name>
    <value>pulsar</value>
</property>

<property>
    <name>io.file.buffer.size</name>
    <value>4096</value>
</property>

<property>
    <name>io.seqfile.compress.blocksize</name>
    <value>1000000</value>
</property>
<property>

    <name>io.seqfile.compression.type</name>
    <value>BLOCK</value>
</property>

<property>
    <name>io.map.index.interval</name>
    <value>128</value>
</property>
```

> #### Tip
>
> For more information about the Hadoop HDFS, see [here](https://hadoop.apache.org/docs/current/).

### Configure filesystem offloader to run automatically

Namespace policy can be configured to offload data automatically once a threshold is reached. The threshold is based on the size of data that a topic has stored on a Pulsar cluster. Once the topic reaches the threshold, an offload operation is triggered automatically. 

Threshold value|Action
|---|---
> 0 | It triggers the offloading operation if the topic storage reaches its threshold.
= 0|It causes a broker to offload data as soon as possible.
< 0 |It disables automatic offloading operation.

Automatic offload runs when a new segment is added to a topic log. If you set the threshold on a namespace, but few messages are being produced to the topic, offloader does not work until the current segment is full.

You can configure the threshold size using CLI tools, such as pulsar-admin.

#### Example

This example sets the filesystem offloader threshold size to 10 MB using pulsar-admin.

```bash
pulsar-admin namespaces set-offload-threshold --size 10M my-tenant/my-namespace
```

> #### Tip
>
> For more information about the `pulsar-admin namespaces set-offload-threshold options` command, including flags, descriptions, default values, and shorthands, see [here](reference-pulsar-admin.md#set-offload-threshold). 

### Configure filesystem offloader to run manually

For individual topics, you can trigger filesystem offloader manually using one of the following methods:

- Use REST endpoint.

- Use CLI tools (such as pulsar-admin). 

To trigger via CLI tools, you need to specify the maximum amount of data (threshold) that should be retained on a Pulsar cluster for a topic. If the size of the topic data on the Pulsar cluster exceeds this threshold, segments from the topic are offloaded to the filesystem until the threshold is no longer exceeded. Older segments are offloaded first.

#### Example

- This example triggers the filesystem offloader to run manually using pulsar-admin.

    ```bash
    pulsar-admin topics offload --size-threshold 10M persistent://my-tenant/my-namespace/topic1
    ``` 

    **Output**

    ```bash
    Offload triggered for persistent://my-tenant/my-namespace/topic1 for messages before 2:0:-1
    ```

    > #### Tip
    >
    > For more information about the `pulsar-admin topics offload options` command, including flags, descriptions, default values, and shorthands, see [here](reference-pulsar-admin.md#offload). 

- This example checks filesystem offloader status using pulsar-admin.

    ```bash
    pulsar-admin topics offload-status persistent://my-tenant/my-namespace/topic1
    ```

    **Output**

    ```bash
    Offload is currently running
    ```

    To wait for the filesystem to complete the job, add the `-w` flag.

    ```bash
    pulsar-admin topics offload-status -w persistent://my-tenant/my-namespace/topic1
    ```

    **Output**
    
    ```
    Offload was a success
    ```

    If there is an error in the offloading operation, the error is propagated to the `pulsar-admin topics offload-status` command.

    ```bash
    pulsar-admin topics offload-status persistent://my-tenant/my-namespace/topic1
    ```

    **Output**

    ```
    Error in offload
    null

    Reason: Error offloading: org.apache.bookkeeper.mledger.ManagedLedgerException: java.util.concurrent.CompletionException: com.amazonaws.services.s3.model.AmazonS3Exception: Anonymous users cannot initiate multipart uploads.  Please authenticate. (Service: Amazon S3; Status Code: 403; Error Code: AccessDenied; Request ID: 798758DE3F1776DF; S3 Extended Request ID: dhBFz/lZm1oiG/oBEepeNlhrtsDlzoOhocuYMpKihQGXe6EG8puRGOkK6UwqzVrMXTWBxxHcS+g=), S3 Extended Request ID: dhBFz/lZm1oiG/oBEepeNlhrtsDlzoOhocuYMpKihQGXe6EG8puRGOkK6UwqzVrMXTWBxxHcS+g=
    ````

    > #### Tip
    >
    > For more information about the `pulsar-admin topics offload-status options` command, including flags, descriptions, default values, and shorthands, see [here](reference-pulsar-admin.md#offload-status). 

## Tutorial

For the complete and step-by-step instructions on how to use the filesystem offloader with Pulsar, see [here](https://hub.streamnative.io/offloaders/filesystem/2.5.1).