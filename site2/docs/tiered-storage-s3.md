---
id: tiered-storage-s3
title: Use S3 offloader with Pulsar
sidebar_label: "S3 offloader"
---

S3 offloader is introduced to serve S3-compatible storage, which means that the storage employs the S3 API as its “language" and applications that speak the S3 API are able to plug and play with S3-compatible storage. 

This chapter guides you through every step of installing and configuring the S3 offloader and using it with Pulsar. 

## Installation

Follow the steps below to install the S3 offloader.

### Prerequisite

- Pulsar: 2.9.3 or later versions
  
### Steps

This example uses Pulsar 2.9.3.

1. [Download the Pulsar tarball](getting-started-standalone.md#install-pulsar-using-binary-release).

2. Download and untar the Pulsar offloaders package, then copy the Pulsar offloaders as `offloaders` in the Pulsar directory. See [Install tiered storage offloaders](getting-started-standalone.md#install-tiered-storage-offloaders-optional).

   **Output**
   
   As shown from the output, Pulsar uses [Apache jclouds](https://jclouds.apache.org) to support [AWS S3](https://aws.amazon.com/s3/), [GCS](https://cloud.google.com/storage/), [Azure](https://portal.azure.com/#home), and [Aliyun OSS](https://www.aliyun.com/product/oss) for long-term storage.

   ```
   
   tiered-storage-file-system-2.9.3.nar
   tiered-storage-jcloud-2.9.3.nar
   
   ```

   :::note

   * If you run Pulsar in a bare-metal cluster, ensure that `offloaders` tarball is unzipped in every broker's Pulsar directory.
   * If you run Pulsar in Docker or deploy Pulsar using a Docker image (such as K8s and DCOS), you can use the `apachepulsar/pulsar-all` image. The `apachepulsar/pulsar-all` image has already bundled tiered storage offloaders.

   :::

## Configuration

:::note

Before offloading data from BookKeeper to S3-compatible storage, you need to configure some properties of the S3 offload driver. Besides, you can also configure the S3 offloader to run it automatically or trigger it manually.

:::


### Configure S3 offloader driver

You can configure the S3 offloader driver in the configuration file `broker.conf` or `standalone.conf`.

- **Required** configurations are as below.
  
  | Required configuration | Description | Example value |
  | --- | --- |--- |
  | `managedLedgerOffloadDriver` | Offloader driver name, which is case-insensitive. | S3 |
  | `offloadersDirectory` | Offloader directory | offloaders |
  | `managedLedgerOffloadBucket` | [Bucket](#bucket-required) | pulsar-topic-offload |
  | `managedLedgerOffloadServiceEndpoint` | [Endpoint](#endpoint-required) | http://localhost:9000 |

- **Optional** configurations are as below.

  | Optional | Description | Default value |
  | --- | --- | --- |
  | `managedLedgerOffloadReadBufferSizeInBytes` | Block size for each individual read when reading back data from S3-compatible storage. | 1 MB |
  | `managedLedgerOffloadMaxBlockSizeInBytes` | Maximum block size sent during a multi-part upload to S3-compatible storage. It **cannot** be smaller than 5 MB. | 64 MB |
  | `managedLedgerMinLedgerRolloverTimeMinutes` | Minimum time between ledger rollover for a topic.<br /><br />**Note**: It's **not** recommended to change the default value in a production environment. | 2 |
  | `managedLedgerMaxEntriesPerLedger` | Maximum number of entries to append to a ledger before triggering a rollover.<br /><br />**Note**: It's **not** recommended to change the default value in a production environment. | 5000 |

#### Bucket (required)

A bucket is a basic container that holds your data. Everything you store in S3-compatible storage must be contained in a bucket. You can use a bucket to organize your data and control access to your data, but unlike directory and folder, you cannot nest a bucket.

##### Example

This example names the bucket `pulsar-topic-offload`.

```conf

managedLedgerOffloadBucket=pulsar-topic-offload

```

#### Endpoint (required) 

The endpoint is the region where a bucket is located.

 
##### Example

This example sets the endpoint as `localhost`.

```

managedLedgerOffloadServiceEndpoint=http://localhost:9000

```

#### Authentication (optional)

To be able to access S3-compatible storage, you need to authenticate with S3-compatible storage.

Set the environment variables `ACCESS_KEY_ID` and `ACCESS_KEY_SECRET` in `conf/pulsar_env.sh`.

```bash

export ACCESS_KEY_ID=ABC123456789
export ACCESS_KEY_SECRET=ded7db27a4558e2ea8bbf0bf37ae0e8521618f366c

```

:::note

Exporting these environment variables makes them available in the environment of spawned processes.

:::

### Run S3 offloader automatically

Namespace policy can be configured to offload data automatically once a threshold is reached. The threshold is based on the size of data that a topic has stored in a Pulsar cluster. Once the topic reaches the threshold, an offloading operation is triggered automatically. 

| Threshold value | Action |
| --- | --- |
| > 0 | It triggers the offloading operation if the topic storage reaches its threshold. |
| = 0 | It causes a broker to offload data as soon as possible. |
| < 0 | It disables automatic offloading operation. |

Automatic offloading runs when a new segment is added to a topic log. If you set the threshold for a namespace, but few messages are being produced to the topic, the offloader does not work until the current segment is full.

You can configure the threshold size using CLI tools, such as [`pulsar-admin`](/tools/pulsar-admin/).

The offload configurations in `broker.conf` and `standalone.conf` are used for the namespaces that do not have namespace-level offload policies. Each namespace can have its offload policy. If you want to set an offload policy for a specific namespace, use the command [`pulsar-admin namespaces set-offload-policies options`](/tools/pulsar-admin/) command.
 
#### Example

This example sets the S3 offloader threshold size to 10 MB using `pulsar-admin`.

```bash

bin/pulsar-admin namespaces set-offload-threshold --size 10M my-tenant/my-namespace

```

:::tip

For more information about the `pulsar-admin namespaces set-offload-threshold options` command, including flags, descriptions, and default values, see [Pulsar admin docs](/tools/pulsar-admin/). 

:::

### Run S3 offloader manually

For individual topics, you can trigger the S3 offloader manually using one of the following methods:

- Use REST endpoint.

- Use CLI tools, such as [`pulsar-admin`](/tools/pulsar-admin/). 

 To trigger it via CLI tools, you need to specify the maximum amount of data (threshold) that should be retained in a Pulsar cluster for a topic. If the size of the topic data in the Pulsar cluster exceeds this threshold, segments from the topic are moved to S3-compatible storage until the threshold is no longer exceeded. Older segments are moved first.

#### Example

- This example triggers the S3 offloader to run manually using `pulsar-admin`.

  ```bash
  
  bin/pulsar-admin topics offload --size-threshold 10M my-tenant/my-namespace/topic1
  
  ```

  **Output**

  ```bash
  
  Offload triggered for persistent://my-tenant/my-namespace/topic1 for messages before 2:0:-1
  
  ```

  :::tip

  For more information about the `pulsar-admin topics offload options` command, including flags, descriptions, and default values, see [Pulsar admin docs](/tools/pulsar-admin/). 

  :::

- This example checks the S3 offloader status using `pulsar-admin`.

  ```bash
  
  bin/pulsar-admin topics offload-status persistent://my-tenant/my-namespace/topic1
  
  ```

  **Output**

  ```bash
  
  Offload is currently running
  
  ```

  To wait for the S3 offloader to complete the job, add the `-w` flag.

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

  Reason: Error offloading: org.apache.bookkeeper.mledger.ManagedLedgerException: java.util.concurrent.CompletionException: com.amazonaws.services.s3.model.AmazonS3Exception: Anonymous users cannot initiate multipart uploads.  Please authenticate. (Service: Amazon S3; Status Code: 403; Error Code: AccessDenied; Request ID: 798758DE3F1776DF; S3 Extended Request ID: dhBFz/lZm1oiG/oBEepeNlhrtsDlzoOhocuYMpKihQGXe6EG8puRGOkK6UwqzVrMXTWBxxHcS+g=), S3 Extended Request ID: dhBFz/lZm1oiG/oBEepeNlhrtsDlzoOhocuYMpKihQGXe6EG8puRGOkK6UwqzVrMXTWBxxHcS+g=
  
  ```

  :::tip

  For more information about the `pulsar-admin topics offload-status options` command, including flags, descriptions, and default values, see [Pulsar admin docs](/tools/pulsar-admin/). 

  :::


