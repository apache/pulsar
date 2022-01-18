---
id: version-2.7.4-concepts-tiered-storage
title: Tiered Storage
sidebar_label: Tiered Storage
original_id: concepts-tiered-storage
---

Pulsar's segment oriented architecture allows for topic backlogs to grow very large, effectively without limit. However, this can become expensive over time.

One way to alleviate this cost is to use Tiered Storage. With tiered storage, older messages in the backlog can be moved from BookKeeper to a cheaper storage mechanism, while still allowing clients to access the backlog as if nothing had changed.

![Tiered Storage](assets/pulsar-tiered-storage.png)

> Data written to BookKeeper is replicated to 3 physical machines by default. However, once a segment is sealed in BookKeeper it becomes immutable and can be copied to long term storage. Long term storage can achieve cost savings by using mechanisms such as [Reed-Solomon error correction](https://en.wikipedia.org/wiki/Reed%E2%80%93Solomon_error_correction) to require fewer physical copies of data.

Pulsar currently supports S3, Google Cloud Storage (GCS), and filesystem for [long term store](https://pulsar.apache.org/docs/en/cookbooks-tiered-storage/). Offloading to long term storage triggered via a Rest API or command line interface. The user passes in the amount of topic data they wish to retain on BookKeeper, and the broker will copy the backlog data to long term storage. The original data will then be deleted from BookKeeper after a configured delay (4 hours by default).

> For a guide for setting up tiered storage, see the [Tiered storage cookbook](cookbooks-tiered-storage.md).
