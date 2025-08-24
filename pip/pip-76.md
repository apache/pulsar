# PIP-76: Streaming Offload

- Status: Discuss
- Authors: RenKai Ge, Yong Zhang, Ran Gao, Penghui Li
- Release: 2.8.0
- Proposal time: 2020-12-23

---

# Motivation

[PIP 17](https://github.com/apache/pulsar/wiki/PIP-17:-Tiered-storage-for-Pulsar-topics) introduced the data offloading mechanism for Pulsar and many users enable the data offloading to reduce the cold data storage costs. But in [PIP 17](https://github.com/apache/pulsar/wiki/PIP-17:-Tiered-storage-for-Pulsar-topics), the data offloading is triggered automatically when rolling over a ledger or manually trigger data offloading through the Pulsar admin CLI. Both of these two ways to offload data are tight coupling with the leger rollover mechanism. So if you want to control the size of each offloaded data block, you need to tune the ledger rollover policy.

There are some drawbacks to the current data offloading mechanism:

1. For the topic with low message write throughput, if the ledger rollover policy is based on the max size of a ledger, the offloaded data will available for reading after a long time. If the ledger rollover policy is based on the time duration, the offloaded data block might too small, this may not be an efficient way to use blob storage.
1. For the topic with high message write throughput, if the ledger rollover policy is based on the max size of a ledger, the ledger rollover might be triggered very frequently, this will bring more overhead to the metadata server(Apache Zookeeper). So in Pulsar, we can specify the min ledger rollover time duration. But if the rollover policy is based on the time duration, the offloaded data block size might become huge. For the historical data reading, we always want to be able to read data as quickly as possible. So it&#39;s better to split the huge blob object into multiple reasonably sized data blocks and read these data blocks in parallel.
1. The data offloading will read the ledger data from the bookies directly, this will bring more overhead to the bookies and might make the message writing latency unstable.

So, this proposal aims to decouple the data offloading and ledger rollover and offload the cached data as much as possible based on [PIP 17](https://github.com/apache/pulsar/wiki/PIP-17:-Tiered-storage-for-Pulsar-topics). And provide a flexible user perspective approach to control the data offloading behavior.

# Approach

In this proposal, we will introduce a segment-based data offloading mechanism for Pulsar. The core idea is distributing the data of the ledgers into the offload segments.

![](https://static.slab.com/prod/uploads/cam7h8fn/posts/images/VgexgMSW237oig_Wwy7NzR4S.png)

As shown in the above illustration, the data of a ledger can be offloaded as several offload segments and the data of several ledgers can be offloaded as an offload segment.

## Offload segment

The offload segment can be determined by offload data size or time duration after the last data offload triggered. From the data size and time duration based segment assignment, users can control both offload segment size and the maximum available for reading time.

The offload assignment is done by the broker and the offload segment is immutable after assigned. Each segment should contain the following data:

- A unique ID to distinguish different segments.
- The assigned timestamp to indicate when the offload segment is assigned.
- The offloaded timestamp to indicate when the offload segment is offloaded.
- The Status of the offload segment(`Assigned | Offloaded | Failed`).
- Start position and end position(ledgerId, entryId).
- Offload driver metadata to locate the offload data.

## Offload data of the segment

The biggest difference between streaming offloader and ledger-based offloader is ledger-based offloader offload data to the tiered storage after the ledger closed, the streaming offloader will continue to offload the new incoming data into the tiered storage ASAP.

So, if streaming offload enabled on a topic and new entries write to this topic, the broker will start a new offload segment and write the new entries to the segment asynchronously, so this will not affect write latency. After the offload segment has been filled(reach the max size of an offload segment or reach the max duration of the offload segment), a new offload segment will be created, and the new entries will write to the new offload segment. Need to explain here is to create a new offload segment must after the last offload closed.

After the offload segment offloaded, the offload status and the offload timestamp should be updated on the metadata server.

## Offload buffer

The offload buffer is an internal component of the streaming offloader that can buffer some entries for offloading to void frequently read data from the bookies. The new incoming entries will be buffered into the offload buffer first and remove from the buffer after offloaded into the tiered storage.

The offload buffer cannot buffer all data written to the topic but does not offload to the tiered storage. If the offloader is currently in a slow offload situation, the buffer might be used up, the broker needs to stop to write new data into the buffer to avoid excessive memory consumption until the data of the buffer get removed and the buffer has extra space. So, in this situation, the broker still needs to read data from the topic for data offloading.

The broker cached the data that writes to the bookies for tailing read, we also it tailing cache. So data cached in the tailing cache, the broker can read from the cache directly for data offloading. Otherwise, the broker needs to read data from bookies.

## How to read offloaded data

Currently, we use `ReadHandle` to read data from tiered storage, in streaming offload, the data of a ledger may cross multiple objects, so a new `ReadHandle` should be implemented to read from multiple offload segments.

## How to delete offloaded data

Currently, we delete the data of the ledger by deleting  the corresponding offloaded object, because ledgers and objects are aligned one by one. But in streaming offload, ledger data may pass through multi segments(objects in blob store), when we delete a ledger, we mark the ledger as deleted and check whether all ledgers in corresponding segments are marked deleted, and actually delete the segments which have its all ledgers mark deleted.

## Work with the CLI manually offload

When manually offload happens through the CLI  on the streaming offloader, the broker will close and complete the current offload segment. After the offload segment completed, the offload data will available for users.

## Offload segment failure handling

The offload segment might unable to successfully write to the tiered storage due to the network failure, broker crashes, topic ownership changed and etc. The broker needs to resume the failed offload segment until the offload segment successfully offloaded. And cannot create a new offload segment before the failed offload segment is processed successfully.

## historical ledgers handling

If users enable the streaming offload for a topic that has historical data, the historical data also need to offload to the tiered storage. The historical data also needs to split into multiple offload segments, this can be achieved by using binary search to decide segment start and end. After all historical segments offloaded, the broker will open a new offload segment for offloading the incoming entries.

## Work with when user set back to old offload method

Users might change the streaming offload to ledger-based offload. For handling this case, we should complete the current offload segment and stop streaming offloading. After that, the ledger-based offloader will start.

# Changes

## Metadata Changes

For the Ledger metadata, we need to record the multiple offload segment metadata under a Ledger since a Ledger might too large and an offload segment can across multiple Ledgers since a Ledger might too small.

So the newly introduced **_OffloadSegment_** is for support multiple offload segments in a Ledger and organized by the **_OffloadContext_**

```
message OffloadContext { // one per ledger
    optional int64 uidMsb = 1; // UUID of the begin segment
    optional int64 uidLsb = 2; //
    optional bool complete = 3;
    optional bool bookkeeperDeleted = 4;
    optional int64 timestamp = 5;
    optional OffloadDriverMetadata driverMetadata = 6;
    repeated OffloadSegment offloadSegment = 7; // new 
}

message OffloadSegment {
  optional int64 uidMsb = 1;
  optional int64 uidLsb = 2;
  optional bool complete = 3;
  optional bool bookkeeperDeleted = 4;
  optional int64 assignedTimestamp = 5;
  optional int64 offloadedTimestamp = 6;
  optional int64 endEntryId = 7;
  optional OffloadDriverMetadata driverMetadata = 8;
}
```

## Blob Object Format Changes

### Block Data

A block has a short header of length 128 bytes, followed by payload data

```
[ magic_word ][ header_len ][ block_len ][ first_entry_id ][ ledger_id ][ padding ]
```

- `magic_word` : 4 bytes, a sequence of bytes used to identify the start of a block, which is **0x26A66D32**
- `header_len` : 8 bytes, the length of the block header (128 for now)
- `block_len` : 8 bytes, the length of the block, including the header
- `first_entry_id` : 8 bytes, entry ID of the first entry in the block
- `ledger_id` : 8 bytes, ledger id of this block, **new added this time**
- `padding` : As many bytes as necessary to bring the header length to the length specified by `header_len`

The payload data is a sequence of entries of the format

```
[ entry_len ][ entry_id ][ entry_data ]
```

- `entry_len` : 4 bytes, the length of the entry
- `entry_id` : 8 bytes, the ID of the entry
- `entry_data` : as many bytes as specified in `entry_len`

Padding may be added at the end to bring the block to the correct size. The payload part is totally the same with [PIP-17](https://github.com/apache/pulsar/wiki/PIP-17:-Tiered-storage-for-Pulsar-topics#payload-format)

### Block Data object

A block data object will be combined by one or more consecutive data blocks, and will be stored with a key that contains it&#39;s UUID.

```
Object key: ${uuid}
```

### Block Index

The index is a short header and then a sequence of mappings.

The index header is

```
[ index_magic_word ][ index_len ][ data_object_length ][ data_header_length ][ index_per_ledger ]
```

- `index_magic_word` : 4 bytes, a sequence of bytes to identify the index, which is **0x3D1FB0BC**
- `index_len` : 4 bytes, the total length of the index in bytes, including the header
- `data_object_length` : 8 bytes, the total length of the data object this index is associated with
- `data_header_length` : 8 bytes, the length of the data block headers in the data object
- `index_per_ledger` : Consecutive indices group by ledger.

The layout of index per ledger is:

```
[ ledger_id ][ index_entry_count ][ ledger_metadata_len ][ ledger_metadata ][ block_indices ]
```

- `ledger_id` : 8 bytes, the corresponding ledger id of this index, **new added**
- `index_entry_count` : 4 bytes, the total number of mappings in the index
- `ledger_metadata_len` : 4 bytes, the length of the ledger metadata
- `ledger_metadata` : the binary representation of the ledger metadata, stored in protobuf format
- `block_indices` : Consecutive block index

The layout of a block index is:

```
[ entry_id ][ part_id ][ offset ]
```

- `entry_id` : the first entry id of the corresponding block
- `part_id` : the number of this block related to its ledger
- `offset` : the position of block data beginning in the block data object

### Block Index Object

A block index object will contain the data of **exactly one block index**, and the index object key is initialed with the key of the corresponding block data object and has a suffix `-index`

```
Index key: ${uuid}-index
```

Also check the format in Figure at

![](https://static.slab.com/prod/uploads/cam7h8fn/posts/images/meMvI5KdpoHXClWyRWlRduPu.svg)

## Interface Change

Introduce a new method `streamingOffload` for **_LedgerOffloader_**. This method will return an **_OffloadHandle_** for writing the streaming data into the tiered storage.

```
public class OffloadResult {
  UUID uuid;
  long beginLegdgerId;
  long endLedgerId;
  long beginEntryId;
  long endEntryId;
}

public interface LedgerOffloader {

    CompletableFuture<OffloadHandle> streamingOffload(UUID uuid, Map<String, String> driverMetadata);
    
    CompletableFuture<ReadHandle> readOffloaded(long ledgerId, MLDataFormats.OffloadContext ledgerContext,
                                                       Map<String, String> offloadDriverMetadata);

    CompletableFuture<Void> deleteOffloaded(UUID uuid, Map<String, String> offloadDriverMetadata);
}
```

Details of **_OffloadHandle_**

```
interface OffloadHandle {

    boolean canOffer(long size);

    PositionImpl lastOffered();

    boolean offerEntry(Entry entry) throws OffloadException;

    CompletableFuture<OffloadResult> getOffloadResultAsync();

}
```

## 

## Configuration changes

- Add a boolean configuration to decide whether to use streaming offload at broker, namespace, topic level, default value will be false, means use the current ledger-based offload.
- Add size config `maxSizePerOffloadSegmentMbytes` to decide when to trigger offloading for streaming offload.
- Add time config `maxOffloadSegmentRolloverTimeMinutes` to decide when to trigger offloading for streaming offload.

# Compatibility

All the metadata changes and the offload data format changes that introduced in the proposal can guarantee backward compatibility, this means the new version Pulsar can read the offloaded data from the old version Pulsar, but the old version Pulsar can&#39;t read the offloaded data from the new version Pulsar if the new version Pulsar enabled streaming offload.

# Migration

## Migrate to streaming offload

For users who want to migrate to streaming offload, they can enable the streaming offload at the broker level, namespace level, or topic level.

- Streaming offload will begin when the last ledger-based offload finished.
- For offloaded data readers, they will distinguish two format storage object by metadata, and choose the suitable decoder for each storage object.

## Rollback to ledger based offload

For users who want to rollback to the ledger-based offload, they can disable the streaming offload at the broker level, namespace level, or topic level.

- Streaming offload will finish after the current ledger closed, and a ledger-based offloader will start consequently.
- For offloaded data readers, they will distinguish two format storage object by metadata, and choose the suitable decoder for each storage object.

In some situations, users might want to roll back to an old version when encountering some unexpected behavior on the new version. If the streaming offload is enabled on the cluster, the data might already be offloaded to the tiered storage with the new data format and the offload metadata format, since the old program can&#39;t read the new format data and metadata, so users must to understand the behavior when rollback to an old version.

If users want to roll back to old Pulsar version when streaming offload have already executed. The offloaded data will be not valid for older version pulsar cluster(because new offload segments are not arranged per ledger), users should delete the offloaded data by cluster command before roll back. Otherwise, old Pulsar cluster will try to read offloaded data and print error log, and the data will stay in storage without usability.

This is because streaming offload uses a newer version of offload context, which will use some of the old offload context fields, but not use the `complete` field(we add a new compelete field in every offload segment and use that one instead). Older version of Pulsar will treat the context like it tried to offload the corresponding ledger but failed, then try to delete the wrong written object, but such object didn&#39;t exist, this will cost storage resource leak, and generate some error log(says the objects you want to delete does not exist) but the cluster is basically in good condition and can offload data in bookkeeper again.

If tiered storage is enabled for old version Pulsar, it will start offloading(in ledger-based) from where ledger still exists in bookkeeper like former offload attempt failed.

### Expected behavior if user switch to old version Pulsar directly

- Invalid data will be stored in tiered storage.
- The cluster will offload the data exists in bookkeeper like former offload behavior started but failed.

### Rollback guide

If you need to rollback to an old version Pulsar cluster, follow this guide can help you to do it quickly produce minimal unexpected behavior.

- First use configurations to ensure most data are still stored in bookkeeper, this will be the only data you can get from old cluster.
- Disable data offloading(for both streaming and ledger-based).
- Switch to old version cluster.
- If you need, you can enable ledger-based offload again, this will produce some error log about can&#39;t find object for some key, but it&#39;s OK and will finished after ledger-based offload done for existing ledgers.
- Remove streaming offloaded data in you storage, you can distinguish them by object name format or bucket name if you give a specific bucket name to them before.

# Tests Plan

Integration Test (Filesystem Offloader)

- Offload mechanism should be  transparent to readers, so the test will included a mixed ledger-based and streaming offload result to read to check the compatibility.
