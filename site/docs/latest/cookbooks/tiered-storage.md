---
title: Tiered Storage
tags: [admin, tiered-storage]
---

Pulsar's **Tiered Storage** feature allows older backlog data to be offloaded to long term storage, thereby freeing up space in BookKeeper and reducing storage costs. This cookbook walks you through using tiered storage in your Pulsar cluster.

Tiered storage currently leverage [Apache Jclouds](https://jclouds.apache.org) to supports [S3](https://aws.amazon.com/s3/) and [Google Cloud Storage](https://cloud.google.com/storage/)(GCS for short) for long term storage. And by Jclouds, it is easy to add more [supported](https://jclouds.apache.org/reference/providers/#blobstore-providers) cloud storage provider in the future.

## When should I use Tiered Storage?

Tiered storage should be used when you have a topic for which you want to keep a very long backlog for a long time. For example, if you have a topic containing user actions which you use to train your recommendation systems, you may want to keep that data for a long time, so that if you change your recommendation algorithm you can rerun it against your full user history.

## The offloading mechanism

A topic in Pulsar is backed by a log, known as a managed ledger. This log is composed of an ordered list of segments. Pulsar only every writes to the final segment of the log. All previous segments are sealed. The data within the segment is immutable. This is known as a segment oriented architecture.

{% include figure.html src="/img/pulsar-tiered-storage.png" alt="Tiered Storage" width="80" %}

The Tiered Storage offloading mechanism takes advantage of this segment oriented architecture. When offloading is requested, the segments of the log are copied, one-by-one, to tiered storage. All segments of the log, apart from the segment currently being written to can be offloaded.

On the broker, the administrator must configure the bucket or credentials for the cloud storage service. The configured bucket must exist before attempting to offload. If it does not exist, the offload operation will fail.

Pulsar uses multi-part objects to upload the segment data. It is possible that a broker could crash while uploading the data. We recommend you add a life cycle rule your bucket to expire incomplete multi-part upload after a day or two to avoid getting charged for incomplete uploads.

## Configuring for S3 and GCS in the broker

Offloading is configured in ```broker.conf```. 

At a minimum, the administrator must configure the driver, the bucket and the authenticating.  There is also some other knobs to configure, like the bucket regions, the max block size in backed storage, etc.

### Configuring the driver

Currently we support driver of types: { "aws-s3", "google-cloud-storage" }, 
{% include admonition.html type="warning" content="Driver names are case-insensitive for driver's name. "s3" and "aws-s3" are similar, with "aws-s3" you just don't need to define the url of the endpoint because it is aligned with region, and default is `s3.amazonaws.com`; while with s3, you must provide the endpoint url by `s3ManagedLedgerOffloadServiceEndpoint`." %}

```conf
managedLedgerOffloadDriver=aws-s3
```

### Configuring the Bucket

On the broker, the administrator must configure the bucket and credentials for the cloud storage service. The configured bucket and credentials must exist before attempting to offload. If it does not exist, the offload operation will fail.

- Regarding driver type "aws-s3", the administrator should configure `s3ManagedLedgerOffloadBucket`.

```conf
s3ManagedLedgerOffloadBucket=pulsar-topic-offload
```

- While regarding driver type "google-cloud-storage", the administrator should configure `gcsManagedLedgerOffloadBucket`.
```conf
gcsManagedLedgerOffloadBucket=pulsar-topic-offload
```

### Configure the Bucket Region

Bucket Region is the region where bucket located. 

Regarding AWS S3, the default region is `US East (N. Virginia)`. Page [AWS Regions and Endpoints](https://docs.aws.amazon.com/general/latest/gr/rande.html) contains more information.

Regarding GCS, buckets are default created in the `us multi-regional location`,  page [Bucket Locations](https://cloud.google.com/storage/docs/bucket-locations) contains more information.

- AWS S3 Region example:

```conf
s3ManagedLedgerOffloadRegion=eu-west-3
```

- GCS Region example:

```conf
gcsManagedLedgerOffloadRegion=europe-west3
```

### Configure the Authenticating

#### Authenticating with AWS S3

To be able to access S3, you need to authenticate with S3. Pulsar does not provide any direct means of configuring authentication for S3, but relies on the mechanisms supported by the [DefaultAWSCredentialsProviderChain](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html).

Once you have created a set of credentials in the AWS IAM console, they can be configured in a number of ways.

1. Set the environment variables **AWS_ACCESS_KEY_ID** and **AWS_SECRET_ACCESS_KEY** in ```conf/pulsar_env.sh```.

```bash
export AWS_ACCESS_KEY_ID=ABC123456789
export AWS_SECRET_ACCESS_KEY=ded7db27a4558e2ea8bbf0bf37ae0e8521618f366c
```

{% include admonition.html type="info" content="\"export\" is important so that the variables are made available in the environment of spawned processes." %}


2. Add the Java system properties *aws.accessKeyId* and *aws.secretKey* to **PULSAR_EXTRA_OPTS** in ```conf/pulsar_env.sh```.

```bash
PULSAR_EXTRA_OPTS="${PULSAR_EXTRA_OPTS} ${PULSAR_MEM} ${PULSAR_GC} -Daws.accessKeyId=ABC123456789 -Daws.secretKey=ded7db27a4558e2ea8bbf0bf37ae0e8521618f366c -Dio.netty.leakDetectionLevel=disabled -Dio.netty.recycler.maxCapacity.default=1000 -Dio.netty.recycler.linkCapacity=1024"
```

3. Set the access credentials in ```~/.aws/credentials```.

```conf
[default]
aws_access_key_id=ABC123456789
aws_secret_access_key=ded7db27a4558e2ea8bbf0bf37ae0e8521618f366c
```

If you are running in EC2 you can also use instance profile credentials, provided through the EC2 metadata service, but that is out of scope for this cookbook.

{% include admonition.html type="warning" content="The broker must be rebooted for credentials specified in pulsar_env to take effect." %}

#### Authenticating with GCS

The administrator need configure `gcsManagedLedgerOffloadServiceAccountKeyFile` in `broker.conf` to get GCS service available.  It is a Json file, which contains GCS credentials of service account key. 
[This page](https://support.google.com/googleapi/answer/6158849) contains more information of how to create this key file for authentication. You could also get more information regarding google cloud [IAM](https://cloud.google.com/storage/docs/access-control/iam).

Usually these are the steps to create the authentication file:
1. Open the API Console Credentials page.
2. If it's not already selected, select the project that you're creating credentials for.
3. To set up a new service account, click New credentials and then select Service account key.
4. Choose the service account to use for the key.
5. Choose whether to download the service account's public/private key as a JSON file that can be loaded by a Google API client library.

Here is an example:
```conf
gcsManagedLedgerOffloadServiceAccountKeyFile="/Users/jia/Downloads/project-804d5e6a6f33.json"
```

### Configure the size of block read/write

Pulsar also provides some knobs to configure the size of requests sent to S3/GCS.

- ```s3ManagedLedgerOffloadMaxBlockSizeInBytes``` and ```gcsManagedLedgerOffloadMaxBlockSizeInBytes``` configures the maximum size of a "part" sent during a multipart upload. This cannot be smaller than 5MB. Default is 64MB.
- ```s3ManagedLedgerOffloadReadBufferSizeInBytes``` and ```gcsManagedLedgerOffloadReadBufferSizeInBytes``` configures the block size for each individual read when reading back data from S3/GCS. Default is 1MB.

In both cases, these should not be touched unless you know what you are doing.


## Configuring offload to run automatically

Namespace policies can be configured to offload data automatically once a threshold is reached. The threshold is based on the size of data that the topic has stored on the pulsar cluster. Once the topic reaches the threshold, an offload operation will be triggered. Setting a negative value to the threshold will disable automatic offloading. Setting the threshold to 0 will cause the broker to offload data as soon as it possiby can.

```bash
$ bin/pulsar-admin namespaces set-offload-threshold --size 10M my-tenant/my-namespace
```

{% include admonition.html type="warning" content="Automatic offload runs when a new segment is added to a topic log. If you set the threshold on a namespace, but few messages are being produced to the topic, offload will not until the current segment is full." %}

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
````
