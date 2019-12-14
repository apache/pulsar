---
id: concepts-tiered-storage
title: Tiered storage
sidebar_label: Tiered storage
---

Pulsar's segment oriented architecture allows for topic backlogs to grow very large effectively without limit. However, this can become expensive over time.

One way to alleviate this cost is to use Tiered Storage with which older messages in the backlog can be moved from BookKeeper to a cheaper storage mechanism, while still allowing clients to access the backlog as if nothing has changed.

Data written to BookKeeper is replicated to 3 physical machines by default. However, once a segment is sealed in BookKeeper it becomes immutable and can be copied to long term storage which can achieve cost savings by using mechanisms like [Reed-Solomon error correction](https://en.wikipedia.org/wiki/Reed%E2%80%93Solomon_error_correction) to require fewer physical copies of data.

Pulsar currently supports S3, Google Cloud Storage (GCS), and filesystem for long term storage. Offloading to long term storage is triggered via a Rest API or command line interface. You can pass in the amount of topic data you wish to retain on BookKeeper, and the broker will copy the backlog data to long term storage. The original data will then be deleted from BookKeeper after a configured delay (4 hours by default).

> For a guide for setting up tiered storage, see [How to manage offloaders](tiered-how-to-manage-offloaders.md).
