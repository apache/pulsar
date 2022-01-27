---
id: version-2.8.2-tiered-storage-azure
title: Use Azure BlobStore offloader with Pulsar
sidebar_label: Azure BlobStore offloader
original_id: tiered-storage-azure
---

This chapter guides you through every step of installing and configuring the Azure BlobStore offloader and using it with Pulsar.

## Installation

Follow the steps below to install the Azure BlobStore offloader.

### Prerequisite

- Pulsar: 2.6.2 or later versions
  
### Step

This example uses Pulsar 2.6.2.

1. Download the Pulsar tarball using one of the following ways:

   * Download from the [Apache mirror](https://archive.apache.org/dist/pulsar/pulsar-2.6.2/apache-pulsar-2.6.2-bin.tar.gz)

   * Download from the Pulsar [downloads page](https://pulsar.apache.org/download)

   * Use [wget](https://www.gnu.org/software/wget):

     ```shell
     wget https://archive.apache.org/dist/pulsar/pulsar-2.6.2/apache-pulsar-2.6.2-bin.tar.gz
     ```

2. Download and untar the Pulsar offloaders package. 

    ```bash
    wget https://downloads.apache.org/pulsar/pulsar-2.6.2/apache-pulsar-offloaders-2.6.2-bin.tar.gz
    tar xvfz apache-pulsar-offloaders-2.6.2-bin.tar.gz
    ```

3. Copy the Pulsar offloaders as `offloaders` in the Pulsar directory.

    ```
    mv apache-pulsar-offloaders-2.6.2/offloaders apache-pulsar-2.6.2/offloaders

    ls offloaders
    ```

    **Output**

    As shown from the output, Pulsar uses [Apache jclouds](https://jclouds.apache.org) to support [AWS S3](https://aws.amazon.com/s3/),  [GCS](https://cloud.google.com/storage/) and [Azure](https://portal.azure.com/#home) for long term storage. 

    ```
    tiered-storage-file-system-2.6.2.nar
    tiered-storage-jcloud-2.6.2.nar
    ```

    > #### Note
    >
    > * If you are running Pulsar in a bare metal cluster, make sure that `offloaders` tarball is unzipped in every broker's Pulsar directory.
    > 
    > * If you are running Pulsar in Docker or deploying Pulsar using a Docker image (such as K8s and DCOS), you can use the `apachepulsar/pulsar-all` image instead of the `apachepulsar/pulsar` image. `apachepulsar/pulsar-all` image has already bundled tiered storage offloaders.

## Configuration

> #### Note
> 
> Before offloading data from BookKeeper to Azure BlobStore, you need to configure some properties of the Azure BlobStore offload driver.

Besides, you can also configure the Azure BlobStore offloader to run it automatically or trigger it manually.

### Configure Azure BlobStore offloader driver

You can configure the Azure BlobStore offloader driver in the configuration file `broker.conf` or `standalone.conf`.

- **Required** configurations are as below.
  
    Required configuration | Description | Example value
    |---|---|---
    `managedLedgerOffloadDriver` | Offloader driver name | azureblob
    `offloadersDirectory` | Offloader directory | offloaders
    `managedLedgerOffloadBucket` | Bucket | pulsar-topic-offload

- **Optional** configurations are as below.

    Optional | Description | Example value
    |---|---|---
    `managedLedgerOffloadReadBufferSizeInBytes`|Size of block read|1 MB
    `managedLedgerOffloadMaxBlockSizeInBytes`|Size of block write|64 MB
    `managedLedgerMinLedgerRolloverTimeMinutes`|Minimum time between ledger rollover for a topic<br><br>**Note**: it is not recommended that you set this configuration in the production environment.|2
    `managedLedgerMaxEntriesPerLedger`|Maximum number of entries to append to a ledger before triggering a rollover.<br><br>**Note**: it is not recommended that you set this configuration in the production environment.|5000

#### Bucket (required)

A bucket is a basic container that holds your data. Everything you store in Azure BlobStore must be contained in a bucket. You can use a bucket to organize your data and control access to your data, but unlike directory and folder, you cannot nest a bucket.

##### Example

This example names the bucket as _pulsar-topic-offload_.

```conf
managedLedgerOffloadBucket=pulsar-topic-offload
```

#### Authentication (required)

To be able to access Azure BlobStore, you need to authenticate with Azure BlobStore.

* Set the environment variables `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_ACCESS_KEY` in `conf/pulsar_env.sh`.

    "export" is important so that the variables are made available in the environment of spawned processes.

    ```bash
    export AZURE_STORAGE_ACCOUNT=ABC123456789
    export AZURE_STORAGE_ACCESS_KEY=ded7db27a4558e2ea8bbf0bf37ae0e8521618f366c
    ```

#### Size of block read/write

You can configure the size of a request sent to or read from Azure BlobStore in the configuration file `broker.conf` or `standalone.conf`. 

Configuration|Description|Default value
|---|---|---
`managedLedgerOffloadReadBufferSizeInBytes`|Block size for each individual read when reading back data from Azure BlobStore store.|1 MB
`managedLedgerOffloadMaxBlockSizeInBytes`|Maximum size of a "part" sent during a multipart upload to Azure BlobStore store. It **cannot** be smaller than 5 MB. |64 MB

### Configure Azure BlobStore offloader to run automatically

Namespace policy can be configured to offload data automatically once a threshold is reached. The threshold is based on the size of data that a topic has stored on a Pulsar cluster. Once the topic reaches the threshold, an offloading operation is triggered automatically. 

Threshold value|Action
|---|---
> 0 | It triggers the offloading operation if the topic storage reaches its threshold.
= 0|It causes a broker to offload data as soon as possible.
< 0 |It disables automatic offloading operation.

Automatic offloading runs when a new segment is added to a topic log. If you set the threshold on a namespace, but few messages are being produced to the topic, offloader does not work until the current segment is full.

You can configure the threshold size using CLI tools, such as pulsar-admin.

The offload configurations in `broker.conf` and `standalone.conf` are used for the namespaces that do not have namespace level offload policies. Each namespace can have its own offload policy. If you want to set offload policy for each namespace, use the command [`pulsar-admin namespaces set-offload-policies options`](https://pulsar.apache.org/tools/pulsar-admin/2.6.0-SNAPSHOT/#-em-set-offload-policies-em-) command.
 
#### Example

This example sets the Azure BlobStore offloader threshold size to 10 MB using pulsar-admin.

```bash
bin/pulsar-admin namespaces set-offload-threshold --size 10M my-tenant/my-namespace
```

> #### Tip
>
> For more information about the `pulsar-admin namespaces set-offload-threshold options` command, including flags, descriptions, and default values, see [here](https://pulsar.apache.org/tools/pulsar-admin/2.6.0-SNAPSHOT/#-em-set-offload-threshold-em-). 

### Configure Azure BlobStore offloader to run manually

For individual topics, you can trigger Azure BlobStore offloader manually using one of the following methods:

- Use REST endpoint.

- Use CLI tools (such as pulsar-admin). 

    To trigger it via CLI tools, you need to specify the maximum amount of data (threshold) that should be retained on a Pulsar cluster for a topic. If the size of the topic data on the Pulsar cluster exceeds this threshold, segments from the topic are moved to Azure BlobStore until the threshold is no longer exceeded. Older segments are moved first.

#### Example

- This example triggers the Azure BlobStore offloader to run manually using pulsar-admin.

    ```bash
    bin/pulsar-admin topics offload --size-threshold 10M my-tenant/my-namespace/topic1
    ``` 

    **Output**

    ```bash
    Offload triggered for persistent://my-tenant/my-namespace/topic1 for messages before 2:0:-1
    ```

    > #### Tip
    >
    > For more information about the `pulsar-admin topics offload options` command, including flags, descriptions, and default values, see [here](https://pulsar.apache.org/tools/pulsar-admin/2.6.0-SNAPSHOT/#-em-offload-em-). 

- This example checks the Azure BlobStore offloader status using pulsar-admin.

    ```bash
    bin/pulsar-admin topics offload-status persistent://my-tenant/my-namespace/topic1
    ```

    **Output**

    ```bash
    Offload is currently running
    ```

    To wait for the Azure BlobStore offloader to complete the job, add the `-w` flag.

    ```bash
    bin/pulsar-admin topics offload-status -w persistent://my-tenant/my-namespace/topic1
    ```

    **Output**
    
    ```
    Offload was a success
    ```

    If there is an error in offloading, the error is propagated to the `pulsar-admin topics offload-status` command.

    ```bash
    bin/pulsar-admin topics offload-status persistent://my-tenant/my-namespace/topic1
    ```

    **Output**

    ```
    Error in offload
    null

    Reason: Error offloading: org.apache.bookkeeper.mledger.ManagedLedgerException: 
    ```

    > #### Tip
    >
    > For more information about the `pulsar-admin topics offload-status options` command, including flags, descriptions, and default values, see [here](https://pulsar.apache.org/tools/pulsar-admin/2.6.0-SNAPSHOT/#-em-offload-status-em-). 
