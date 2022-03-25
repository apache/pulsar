---
id: version-2.7.2-tiered-storage-overview
title: Overview of tiered storage
sidebar_label: Overview
original_id: tiered-storage-overview
---

Pulsar's **Tiered Storage** feature allows older backlog data to be moved from BookKeeper to long term and cheaper storage, while still allowing clients to access the backlog as if nothing has changed. 

* Tiered storage uses [Apache jclouds](https://jclouds.apache.org) to support [Amazon S3](https://aws.amazon.com/s3/) and [GCS (Google Cloud Storage)](https://cloud.google.com/storage/) for long term storage. 

    With jclouds, it is easy to add support for more [cloud storage providers](https://jclouds.apache.org/reference/providers/#blobstore-providers) in the future.

    > #### Tip
    > 
    > For more information about how to use the AWS S3 offloader with Pulsar, see [here](tiered-storage-aws.md).
    > 
    > For more information about how to use the GCS offloader with Pulsar, see [here](tiered-storage-gcs.md).

* Tiered storage uses [Apache Hadoop](http://hadoop.apache.org/) to support filesystems for long term storage. 

    With Hadoop, it is easy to add support for more filesystems in the future.

    > #### Tip
    > 
    > For more information about how to use the filesystem offloader with Pulsar, see [here](tiered-storage-filesystem.md).

## When to use tiered storage?

Tiered storage should be used when you have a topic for which you want to keep a very long backlog for a long time. 

For example, if you have a topic containing user actions which you use to train your recommendation systems, you may want to keep that data for a long time, so that if you change your recommendation algorithm, you can rerun it against your full user history.

## How does tiered storage work?

A topic in Pulsar is backed by a **log**, known as a **managed ledger**. This log is composed of an ordered list of segments. Pulsar only writes to the final segment of the log. All previous segments are sealed. The data within the segment is immutable. This is known as a **segment oriented architecture**.

![Tiered storage](assets/pulsar-tiered-storage.png "Tiered Storage")

The tiered storage offloading mechanism takes advantage of segment oriented architecture. When offloading is requested, the segments of the log are copied one-by-one to tiered storage. All segments of the log (apart from the current segment) written to tiered storage can be offloaded.

Data written to BookKeeper is replicated to 3 physical machines by default. However, once a segment is sealed in BookKeeper, it becomes immutable and can be copied to long term storage. Long term storage can achieve cost savings by using mechanisms such as [Reed-Solomon error correction](https://en.wikipedia.org/wiki/Reed%E2%80%93Solomon_error_correction) to require fewer physical copies of data.

Before offloading ledgers to long term storage, you need to configure buckets, credentials, and other properties for the cloud storage service. Additionally, Pulsar uses multi-part objects to upload the segment data and brokers may crash while uploading the data. It is recommended that you add a life cycle rule for your bucket to expire incomplete multi-part upload after a day or two days to avoid getting charged for incomplete uploads. Moreover, you can trigger the offloading operation manually (via REST API or CLI) or automatically (via CLI).  

After offloading ledgers to long term storage, you can still query data in the offloaded ledgers with Pulsar SQL.

For more information about tiered storage for Pulsar topics, see [here](https://github.com/apache/pulsar/wiki/PIP-17:-Tiered-storage-for-Pulsar-topics).
