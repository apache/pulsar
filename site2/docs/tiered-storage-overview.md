---
id: tiered-storage-overview
title: Overview
sidebar_label: Overview
---

Pulsar's **Tiered Storage** feature allows older backlog data to be offloaded to long term storage, thereby freeing up space in BookKeeper and reducing storage costs. This chapter walks you through using tiered storage in your Pulsar cluster.

You can use tiered storage when you have a topic for which you want to keep a very long backlog for a long time. For example, if you have a topic containing user actions to train your recommendation systems, you may want to keep that data for a long time, so that if you change your recommendation algorithm you can rerun it against your full user history.



* Tiered storage uses [Apache jclouds](https://jclouds.apache.org) to support
[Amazon S3](https://aws.amazon.com/s3/) and [Google Cloud Storage](https://cloud.google.com/storage/) (GCS for short)
for long term storage. With Jclouds, it is easy to add support for more
[cloud storage providers](https://jclouds.apache.org/reference/providers/#blobstore-providers) in the future.

* Tiered storage uses [Apache Hadoop](http://hadoop.apache.org/) to support filesystem for long term storage. 
With Hadoop, it is easy to add support for more filesystems in the future.

## Contents

- [Principle of offloaders](tiered-principle-of-offloaders.md)
- [How to manage offloaders](tiered-how-to-manage-offloaders.md)
- [How to use offloaders](tiered-how-to-use-offloaders.md)

