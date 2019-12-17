---
id: tiered-how-to-manage-offloaders
title: How to manage offloaders
sidebar_label: How to manage offloaders
---

Offload is configured in ```broker.conf```.

The administrator must configure the driver, the bucket and the authenticating credentials.
There are also some other knobs to configure, such as the bucket region, the max block size in backed storage, etc.

Currently we support following driver types:

- `aws-s3`: [Simple Cloud Storage Service](https://aws.amazon.com/s3/)
- `google-cloud-storage`: [Google Cloud Storage](https://cloud.google.com/storage/)
- `filesystem`: [Filesystem Storage](http://hadoop.apache.org/)

> Driver names are case-insensitive. There is a fourth driver type, `s3`, which is identical to `aws-s3`,
> though it requires that you specify an endpoint url using `s3ManagedLedgerOffloadServiceEndpoint`. It is applicable other than AWS if
> an S3 compatible data storage is used.

```conf
managedLedgerOffloadDriver=aws-s3
```

## "aws-s3" Driver configuration

### Bucket and Region

Buckets are basic containers that hold your data,
and everything that you store in Cloud Storage must be contained in it.
You can use it to organize your data and control access to your data,
but unlike directories and folders, you cannot nest buckets.

```conf
s3ManagedLedgerOffloadBucket=pulsar-topic-offload
```

Bucket Region is the region where bucket is located. Bucket Region is not a required
but a recommended configuration. If it is not configured, It will use the default region.

With AWS S3, the default region is `US East (N. Virginia)`. Page
[AWS Regions and Endpoints](https://docs.aws.amazon.com/general/latest/gr/rande.html) contains more information.

```conf
s3ManagedLedgerOffloadRegion=eu-west-3
```

### Authentication with AWS

To be able to access AWS S3, you need to authenticate with AWS S3.
Pulsar does not provide any direct means of configuring authentication for AWS S3,
but relies on the mechanisms supported by the
[DefaultAWSCredentialsProviderChain](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html).

Once you have created a set of credentials in the AWS IAM console, you can configure them in a number of ways.

1. Using ec2 instance metadata credentials

If you are on AWS instance with an instance profile that provides credentials, Pulsar will use these credentials
if no any other mechanism is provided.

2. Set the environment variables **AWS_ACCESS_KEY_ID** and **AWS_SECRET_ACCESS_KEY** in ```conf/pulsar_env.sh```.

```bash
export AWS_ACCESS_KEY_ID=ABC123456789
export AWS_SECRET_ACCESS_KEY=ded7db27a4558e2ea8bbf0bf37ae0e8521618f366c
```

> \"export\" is important so that the variables are made available in the environment of spawned processes.


3. Add the Java system properties *aws.accessKeyId* and *aws.secretKey* to **PULSAR_EXTRA_OPTS** in `conf/pulsar_env.sh`.

```bash
PULSAR_EXTRA_OPTS="${PULSAR_EXTRA_OPTS} ${PULSAR_MEM} ${PULSAR_GC} -Daws.accessKeyId=ABC123456789 -Daws.secretKey=ded7db27a4558e2ea8bbf0bf37ae0e8521618f366c -Dio.netty.leakDetectionLevel=disabled -Dio.netty.recycler.maxCapacity.default=1000 -Dio.netty.recycler.linkCapacity=1024"
```

4. Set the access credentials in ```~/.aws/credentials```.

```conf
[default]
aws_access_key_id=ABC123456789
aws_secret_access_key=ded7db27a4558e2ea8bbf0bf37ae0e8521618f366c
```

5. Assuming an IAM role

If you want to assume an IAM role, this can be done via specifying the following:

```conf
s3ManagedLedgerOffloadRole=<aws role arn>
s3ManagedLedgerOffloadRoleSessionName=pulsar-s3-offload
```

This will use the `DefaultAWSCredentialsProviderChain` for assuming this role.

> The broker must be rebooted for credentials specified in pulsar_env to take effect.

### Configuring the size of block read/write

Pulsar also provides some knobs to configure the size of requests sent to AWS S3.

- ```s3ManagedLedgerOffloadMaxBlockSizeInBytes```  configures the maximum size of
  a "part" sent during a multipart upload. This cannot be smaller than 5MB. Default is 64MB.
- ```s3ManagedLedgerOffloadReadBufferSizeInBytes``` configures the block size for
  each individual read when reading back data from AWS S3. Default is 1MB.

In both cases, these should not be touched unless you know what you are doing.

## "google-cloud-storage" Driver configuration

Buckets are the basic containers that hold your data, and everything that you store in
Cloud Storage must be contained in a bucket. You can use buckets to organize your data and
control access to your data, but unlike directories and folders, you cannot nest buckets.

```conf
gcsManagedLedgerOffloadBucket=pulsar-topic-offload
```

Bucket Region is the region where bucket is located. Bucket Region is not a required but
a recommended configuration. If it is not configured, it will use the default region.

Regarding GCS, buckets are default created in the `us multi-regional location`,
page [Bucket Locations](https://cloud.google.com/storage/docs/bucket-locations) contains more information.

```conf
gcsManagedLedgerOffloadRegion=europe-west3
```

### Authentication with GCS

The administrator needs to configure `gcsManagedLedgerOffloadServiceAccountKeyFile` in `broker.conf`
for the broker to be able to access the GCS service. `gcsManagedLedgerOffloadServiceAccountKeyFile` is
a JSON file, containing the GCS credentials of a service account.
[Service Accounts section of this page](https://support.google.com/googleapi/answer/6158849) contains
more information of how to create this key file for authentication. More information about google cloud IAM
is available [here](https://cloud.google.com/storage/docs/access-control/iam).

Usually these are the steps to create the authentication file:
1. Open the API Console Credentials page.
2. If it's not already selected, select the project that you're creating credentials for.
3. To set up a new service account, click New credentials and then select Service account key.
4. Choose the service account to use for the key.
5. Download the service account's public/private key as a JSON file that can be loaded by a Google API client library.

```conf
gcsManagedLedgerOffloadServiceAccountKeyFile="/Users/hello/Downloads/project-804d5e6a6f33.json"
```

### Configuring the size of block read/write

Pulsar also provides some knobs to configure the size of requests sent to GCS.

- ```gcsManagedLedgerOffloadMaxBlockSizeInBytes``` configures the maximum size of a "part" sent
  during a multipart upload. This cannot be smaller than 5MB. Default is 64MB.
- ```gcsManagedLedgerOffloadReadBufferSizeInBytes``` configures the block size for each individual
  read when reading back data from GCS. Default is 1MB.

In both cases, these should not be touched unless you know what you are doing.

## "filesystem" Driver configuration


### Configure connection address

You can configure the connection address in the `broker.conf` file.

```conf
fileSystemURI="hdfs://127.0.0.1:9000"
```
### Configure Hadoop profile path

The configuration file is stored in the Hadoop profile path. It contains various settings, such as base path, authentication, and so on.

```conf
fileSystemProfilePath="../conf/filesystem_offload_core_site.xml"
```

The model for storing topic data uses `org.apache.hadoop.io.MapFile`. You can use all of the configurations in `org.apache.hadoop.io.MapFile` for Hadoop.

**Example**

```conf

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

For more information about the configurations in `org.apache.hadoop.io.MapFile`, see [Filesystem Storage](http://hadoop.apache.org/).