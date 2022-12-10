---
id: tiered-storage-overview
title: Overview of tiered storage
sidebar_label: "Overview"
---

Pulsar's **Tiered Storage** feature allows older backlog data to be moved from BookKeeper to long-term and cheaper storage, while still allowing clients to access the backlog as if nothing has changed.

* Tiered storage uses [Apache jclouds](https://jclouds.apache.org) to support [Amazon S3](https://aws.amazon.com/s3/), [GCS (Google Cloud Storage)](https://cloud.google.com/storage/), [Azure](https://azure.microsoft.com/en-us/services/storage/blobs/) and [Aliyun OSS](https://www.aliyun.com/product/oss) for long-term storage.
  * Read how to [Use AWS S3 offloader with Pulsar](tiered-storage-aws.md);
  * Read how to [Use GCS offloader with Pulsar](tiered-storage-gcs.md);
  * Read how to [Use Azure BlobStore offloader with Pulsar](tiered-storage-azure.md);
  * Read how to [Use Aliyun OSS offloader with Pulsar](tiered-storage-aliyun.md);
  * Read how to [Use S3 offloader with Pulsar](tiered-storage-s3.md).
* Tiered storage uses [Apache Hadoop](http://hadoop.apache.org/) to support filesystems for long-term storage.
  * Read how to [Use filesystem offloader with Pulsar](tiered-storage-filesystem.md).

:::tip

The [AWS S3 offloader](tiered-storage-aws.md) registers specific AWS metadata, such as regions and service URLs and requests bucket location before performing any operations. If you cannot access the Amazon service, you can use the [S3 offloader](tiered-storage-s3.md) instead since it is an S3 compatible API without the metadata.

:::


## When to use tiered storage?

Tiered storage should be used when you have a topic for which you want to keep a very long backlog for a long time.

For example, if you have a topic containing user actions that you use to train your recommendation systems, you may want to keep that data for a long time, so that if you change your recommendation algorithm, you can rerun it against your full user history.

## How to install tiered storage offloaders?

Pulsar releases a separate binary distribution, containing the tiered storage offloaders. To enable those offloaders, you need to download the offloaders tarball release:

```bash
wget pulsar:offloader_release_url
```

After you download the tarball, untar the offloaders package and copy the offloaders as `offloaders` in the pulsar directory:

```bash
tar xvfz apache-pulsar-offloaders-@pulsar:version@-bin.tar.gz
mv apache-pulsar-offloaders-@pulsar:version@/offloaders offloaders

ls offloaders
# tiered-storage-file-system-@pulsar:version@.nar
# tiered-storage-jcloud-@pulsar:version@.nar
```

For more information on how to configure tiered storage, see [Tiered storage cookbook](cookbooks-tiered-storage.md).

:::note

* If you are running Pulsar in a bare metal cluster, make sure that `offloaders` tarball is unzipped in every broker's pulsar directory.
* If you are [running Pulsar in Docker](getting-started-docker.md) or deploying Pulsar using a docker image (e.g. [K8S](deploy-kubernetes.md)), you can use the `apachepulsar/pulsar-all` image instead of the `apachepulsar/pulsar` image. `apachepulsar/pulsar-all` image has already bundled tiered storage offloaders.

:::

## How does tiered storage work?

A topic in Pulsar is backed by a **log**, known as a **managed ledger**. This log is composed of an ordered list of segments. Pulsar only writes to the final segment of the log. All previous segments are sealed. The data within the segment is immutable. This is known as a **segment-oriented architecture**.

![Tiered storage](/assets/pulsar-tiered-storage.png "Tiered Storage")

The tiered storage offloading mechanism takes advantage of the segment-oriented architecture. When offloading is requested, the segments of the log are copied one by one to tiered storage. All segments of the log (apart from the current segment) written to tiered storage can be offloaded.

Data written to BookKeeper is replicated to 3 physical machines by default. However, once a segment is sealed in BookKeeper, it becomes immutable and can be copied to long-term storage. Long-term storage has the potential to achieve significant cost savings.

Before offloading ledgers to long-term storage, you need to configure buckets, credentials, and other properties for the cloud storage service. Additionally, Pulsar uses multi-part objects to upload the segment data and brokers may crash while uploading the data. It is recommended that you add a life cycle rule for your bucket to expire incomplete multi-part upload after a day or two days to avoid getting charged for incomplete uploads. Moreover, you can trigger the offloading operation manually (via REST API or CLI) or automatically (via CLI).

After offloading ledgers to long-term storage, you can still query data in the offloaded ledgers with Pulsar SQL.

For more information about tiered storage for Pulsar topics, see [PIP-17](https://github.com/apache/pulsar/wiki/PIP-17:-Tiered-storage-for-Pulsar-topics) and [offload metrics](reference-metrics.md#offload-metrics).