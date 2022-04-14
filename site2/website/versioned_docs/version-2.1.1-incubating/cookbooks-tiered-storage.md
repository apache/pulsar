---
id: version-2.1.1-incubating-cookbooks-tiered-storage
title: Tiered Storage
sidebar_label: Tiered Storage
original_id: cookbooks-tiered-storage
---

Pulsar's **Tiered Storage** feature allows older backlog data to be offloaded to long term storage, thereby freeing up space in BookKeeper and reducing storage costs. This cookbook walks you through using tiered storage in your Pulsar cluster.

## When should I use Tiered Storage?

Tiered storage should be used when you have a topic for which you want to keep a very long backlog for a long time. For example, if you have a topic containing user actions which you use to train your recommendation systems, you may want to keep that data for a long time, so that if you change your recommendation algorithm you can rerun it against your full user history.

## The offloading mechanism

A topic in Pulsar is backed by a log, known as a managed ledger. This log is composed of an ordered list of segments. Pulsar only every writes to the final segment of the log. All previous segments are sealed. The data within the segment is immutable. This is known as a segment oriented architecture.

![Tiered storage](assets/pulsar-tiered-storage.png "Tiered Storage")

The Tiered Storage offloading mechanism takes advantage of this segment oriented architecture. When offloading is requested, the segments of the log are copied, one-by-one, to tiered storage. All segments of the log, apart from the segment currently being written to can be offloaded.

## Amazon S3

Tiered storage currently supports S3 for long term storage. On the broker, the administrator must configure a S3 bucket and the AWS region where the bucket exists. Offloaded data will be placed into this bucket.

The configured S3 bucket must exist before attempting to offload. If it does not exist, the offload operation will fail.

Pulsar users multipart objects to update the segment data. It is possible that a broker could crash while uploading the data. We recommend you add a lifecycle rule your S3 bucket to expire incomplete multipart upload after a day or two to avoid getting charged for incomplete uploads.

### Configuring the broker

Offloading is configured in ```broker.conf```. 

At a minimum, the user must configure the driver, the region and the bucket.

```conf
managedLedgerOffloadDriver=S3
s3ManagedLedgerOffloadRegion=eu-west-3
s3ManagedLedgerOffloadBucket=pulsar-topic-offload
```

It is also possible to specify the s3 endpoint directly, using `s3ManagedLedgerOffloadServiceEndpoint`. This is useful if you are using a non-AWS storage service which provides an S3 compatible API. 

> If the endpoint is specified directly, then the region must _not_ be set.

> The broker.conf of all brokers must have the same configuration for driver, region and bucket for offload to avoid data becoming unavailable as topics move from one broker to another.

Pulsar also provides some knobs to configure the size of requests sent to S3.

- `s3ManagedLedgerOffloadMaxBlockSizeInBytes` configures the maximum size of a "part" sent during a multipart upload. This cannot be smaller than 5MB. Default is 64MB.
- `s3ManagedLedgerOffloadReadBufferSizeInBytes` configures the block size for each individual read when reading back data from S3. Default is 1MB.

In both cases, these should not be touched unless you know what you are doing.

> The broker must be rebooted for any changes in the configuration to take effect.

### Authenticating with S3

To be able to access S3, you need to authenticate with S3. Pulsar does not provide any direct means of configuring authentication for S3, but relies on the mechanisms supported by the [DefaultAWSCredentialsProviderChain](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html).

Once you have created a set of credentials in the AWS IAM console, they can be configured in a number of ways.

1. Set the environment variables **AWS_ACCESS_KEY_ID** and **AWS_SECRET_ACCESS_KEY** in ```conf/pulsar_env.sh```.

```bash
export AWS_ACCESS_KEY_ID=ABC123456789
export AWS_SECRET_ACCESS_KEY=ded7db27a4558e2ea8bbf0bf37ae0e8521618f366c
```

> \"export\" is important so that the variables are made available in the environment of spawned processes.


2. Add the Java system properties *aws.accessKeyId* and *aws.secretKey* to **PULSAR_EXTRA_OPTS** in `conf/pulsar_env.sh`.

```bash
PULSAR_EXTRA_OPTS="${PULSAR_EXTRA_OPTS} ${PULSAR_MEM} ${PULSAR_GC} -Daws.accessKeyId=ABC123456789 -Daws.secretKey=ded7db27a4558e2ea8bbf0bf37ae0e8521618f366c -Dio.netty.leakDetectionLevel=disabled -Dio.netty.recycler.maxCapacityPerThread=4096"
```

3. Set the access credentials in ```~/.aws/credentials```.

```conf
[default]
aws_access_key_id=ABC123456789
aws_secret_access_key=ded7db27a4558e2ea8bbf0bf37ae0e8521618f366c
```

If you are running in EC2 you can also use instance profile credentials, provided through the EC2 metadata service, but that is out of scope for this cookbook.

> The broker must be rebooted for credentials specified in pulsar_env to take effect.

## Configuring offload to run automatically

Namespace policies can be configured to offload data automatically once a threshold is reached. The threshold is based on the size of data that the topic has stored on the pulsar cluster. Once the topic reaches the threshold, an offload operation will be triggered. Setting a negative value to the threshold will disable automatic offloading. Setting the threshold to 0 will cause the broker to offload data as soon as it possiby can.

```bash
$ bin/pulsar-admin namespaces set-offload-threshold --size 10M my-tenant/my-namespace
```

> Automatic offload runs when a new segment is added to a topic log. If you set the threshold on a namespace, but few messages are being produced to the topic, offload will not until the current segment is full.


## Triggering offload manually

Offloading can manually triggered through a REST endpoint on the Pulsar broker. We provide a CLI which will call this rest endpoint for you.

When triggering offload, you must specify the maximum size, in bytes, of backlog which will be retained locally on the bookkeeper. The offload mechanism will offload segments from the start of the topic backlog until this condition is met.

```bash
$ bin/pulsar-admin topics offload --size-threshold 10M my-tenant/my-namespace/topic1
Offload triggered for persistent://my-tenant/my-namespace/topic1 for messages before 2:0:-1
```

The command to triggers an offload will not wait until the offload operation has completed. To check the status of the offload, use offload-status.

```bash
$ bin/pulsar-admin topics offload-status my-tenant/my-namespace/topic1
Offload is currently running
```

To wait for offload to complete, add the -w flag.

```bash
$ bin/pulsar-admin topics offload-status -w my-tenant/my-namespace/topic1
Offload was a success
```

If there is an error offloading, the error will be propagated to the offload-status command.

```bash
$ bin/pulsar-admin topics offload-status persistent://public/default/topic1                                                                                                       
Error in offload
null

Reason: Error offloading: org.apache.bookkeeper.mledger.ManagedLedgerException: java.util.concurrent.CompletionException: com.amazonaws.services.s3.model.AmazonS3Exception: Anonymous users cannot initiate multipart uploads.  Please authenticate. (Service: Amazon S3; Status Code: 403; Error Code: AccessDenied; Request ID: 798758DE3F1776DF; S3 Extended Request ID: dhBFz/lZm1oiG/oBEepeNlhrtsDlzoOhocuYMpKihQGXe6EG8puRGOkK6UwqzVrMXTWBxxHcS+g=), S3 Extended Request ID: dhBFz/lZm1oiG/oBEepeNlhrtsDlzoOhocuYMpKihQGXe6EG8puRGOkK6UwqzVrMXTWBxxHcS+g=
```

