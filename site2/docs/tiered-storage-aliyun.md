---
id: tiered-storage-aliyun
title: Use Aliyun OSS offloader with Pulsar
sidebar_label: Aliyun OSS offloader
---

This chapter guides you through every step of installing and configuring the Aliyun Object Storage Service (OSS) offloader and using it with Pulsar.

## Installation

Follow the steps below to install the Aliyun OSS offloader.

### Prerequisite

- Pulsar: 2.8.0 or later versions
  
### Step

This example uses Pulsar 2.8.0.

1. Download the Pulsar tarball, see [here](https://pulsar.apache.org/docs/en/standalone/#install-pulsar-using-binary-release).

2. Download and untar the Pulsar offloaders package, then copy the Pulsar offloaders as `offloaders` in the Pulsar directory, see [here](https://pulsar.apache.org/docs/en/standalone/#install-tiered-storage-offloaders-optional).

    **Output**
    
    As shown from the output, Pulsar uses [Apache jclouds](https://jclouds.apache.org) to support [AWS S3](https://aws.amazon.com/s3/), [GCS](https://cloud.google.com/storage/), [Azure](https://portal.azure.com/#home), and [Aliyun OSS](https://www.aliyun.com/product/oss) for long-term storage.
    
    ```
    tiered-storage-file-system-2.8.0.nar
    tiered-storage-jcloud-2.8.0.nar
    ```

    > **Note**
    >
    > * If you are running Pulsar in a bare-metal cluster, make sure that `offloaders` tarball is unzipped in every broker's Pulsar directory.
    > 
    > * If you are running Pulsar in Docker or deploying Pulsar using a Docker image (such as K8s and DCOS), you can use the `apachepulsar/pulsar-all` image. The `apachepulsar/pulsar-all` image has already bundled tiered storage offloaders.

## Configuration

> **Note**
> 
> Before offloading data from BookKeeper to Aliyun OSS, you need to configure some properties of the Aliyun OSS offload driver.

Besides, you can also configure the Aliyun OSS offloader to run it automatically or trigger it manually.

### Configure Aliyun OSS offloader driver

You can configure the Aliyun OSS offloader driver in the configuration file `broker.conf` or `standalone.conf`.

- **Required** configurations are as below.
  
    | Required configuration | Description | Example value |
    | --- | --- |--- |
    | `managedLedgerOffloadDriver` | Offloader driver name, which is case-insensitive. | aliyun-oss |
    | `offloadersDirectory` | Offloader directory | offloaders |
    | `managedLedgerOffloadBucket` | Bucket | pulsar-topic-offload |
    | `managedLedgerOffloadServiceEndpoint` | Endpoint | http://oss-cn-hongkong.aliyuncs.com |

- **Optional** configurations are as below.

    | Optional | Description | Example value |
    | --- | --- | --- |
    | `managedLedgerOffloadReadBufferSizeInBytes` | Size of block read | 1 MB |
    | `managedLedgerOffloadMaxBlockSizeInBytes` | Size of block write | 64 MB |
    | `managedLedgerMinLedgerRolloverTimeMinutes` | Minimum time between ledger rollover for a topic<br><br>**Note**: it is not recommended that you set this configuration in the production environment. | 2 |
    | `managedLedgerMaxEntriesPerLedger` | Maximum number of entries to append to a ledger before triggering a rollover.<br><br>**Note**: it is not recommended that you set this configuration in the production environment. | 5000 |

#### Bucket (required)

A bucket is a basic container that holds your data. Everything you store in Aliyun OSS must be contained in a bucket. You can use a bucket to organize your data and control access to your data, but unlike directory and folder, you cannot nest a bucket.

##### Example

This example names the bucket as _pulsar-topic-offload_.

```conf
managedLedgerOffloadBucket=pulsar-topic-offload
```

#### Endpoint (required) 

The endpoint is the region where a bucket is located.

> **Tip**
>
> For more information about Aliyun OSS regions and endpoints,  see [International website](https://www.alibabacloud.com/help/doc-detail/31837.htm) or [Chinese website](https://help.aliyun.com/document_detail/31837.html).
 
##### Example

This example sets the endpoint as _oss-us-west-1-internal_.

```
managedLedgerOffloadServiceEndpoint=http://oss-us-west-1-internal.aliyuncs.com
```

#### Authentication (required)

To be able to access Aliyun OSS, you need to authenticate with Aliyun OSS.

Set the environment variables `ALIYUN_OSS_ACCESS_KEY_ID` and `ALIYUN_OSS_ACCESS_KEY_SECRET` in `conf/pulsar_env.sh`.

"export" is important so that the variables are made available in the environment of spawned processes.

```bash
export ALIYUN_OSS_ACCESS_KEY_ID=ABC123456789
export ALIYUN_OSS_ACCESS_KEY_SECRET=ded7db27a4558e2ea8bbf0bf37ae0e8521618f366c
```

#### Size of block read/write

You can configure the size of a request sent to or read from Aliyun OSS in the configuration file `broker.conf` or `standalone.conf`. 

| Configuration | Description | Default value |
| --- | --- | --- |
| `managedLedgerOffloadReadBufferSizeInBytes` | Block size for each individual read when reading back data from Aliyun OSS. | 1 MB |
| `managedLedgerOffloadMaxBlockSizeInBytes` | Maximum size of a "part" sent during a multipart upload to Aliyun OSS. It **cannot** be smaller than 5 MB.  | 64 MB |

### Run Aliyun OSS offloader automatically

Namespace policy can be configured to offload data automatically once a threshold is reached. The threshold is based on the size of data that a topic has stored on a Pulsar cluster. Once the topic reaches the threshold, an offloading operation is triggered automatically. 

| Threshold value | Action |
| --- | --- |
| > 0 | It triggers the offloading operation if the topic storage reaches its threshold. |
| = 0 | It causes a broker to offload data as soon as possible. |
| < 0 | It disables automatic offloading operation. |

Automatic offloading runs when a new segment is added to a topic log. If you set the threshold on a namespace, but few messages are being produced to the topic, the offloader does not work until the current segment is full.

You can configure the threshold size using CLI tools, such as pulsar-admin.

The offload configurations in `broker.conf` and `standalone.conf` are used for the namespaces that do not have namespace level offload policies. Each namespace can have its own offload policy. If you want to set offload policy for each namespace, use the command [`pulsar-admin namespaces set-offload-policies options`](https://pulsar.apache.org/tools/pulsar-admin/2.6.0-SNAPSHOT/#-em-set-offload-policies-em-) command.
 
#### Example

This example sets the Aliyun OSS offloader threshold size to 10 MB using pulsar-admin.

```bash
bin/pulsar-admin namespaces set-offload-threshold --size 10M my-tenant/my-namespace
```

> **Tip**
>
> For more information about the `pulsar-admin namespaces set-offload-threshold options` command, including flags, descriptions, and default values, see [here](https://pulsar.apache.org/tools/pulsar-admin/2.6.0-SNAPSHOT/#-em-set-offload-threshold-em-). 

### Run Aliyun OSS offloader manually

For individual topics, you can trigger the Aliyun OSS offloader manually using one of the following methods:

- Use REST endpoint.

- Use CLI tools (such as pulsar-admin). 

    To trigger it via CLI tools, you need to specify the maximum amount of data (threshold) that should be retained on a Pulsar cluster for a topic. If the size of the topic data on the Pulsar cluster exceeds this threshold, segments from the topic are moved to Aliyun OSS until the threshold is no longer exceeded. Older segments are moved first.

#### Example

- This example triggers the Aliyun OSS offloader to run manually using pulsar-admin.

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

- This example checks the Aliyun OSS offloader status using pulsar-admin.

    ```bash
    bin/pulsar-admin topics offload-status persistent://my-tenant/my-namespace/topic1
    ```

    **Output**

    ```bash
    Offload is currently running
    ```

    To wait for the Aliyun OSS offloader to complete the job, add the `-w` flag.

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
    ````

    > **Tip**
    >
    > For more information about the `pulsar-admin topics offload-status options` command, including flags, descriptions, and default values, see [here](https://pulsar.apache.org/tools/pulsar-admin/2.6.0-SNAPSHOT/#-em-offload-status-em-). 
