# PIP-17: Tiered storage for Pulsar topics

* **Status**: Implemented
* **Authors**: Ivan Kelly
* **Mailing List discussion**: https://lists.apache.org/thread.html/3f1c98658395c92e3c858ff966aa829dba0e165a28ef350e0ec3a33f@%3Cdev.pulsar.apache.org%3E
* **Prototype**: https://github.com/ivankelly/incubator-pulsar/tree/s3-offload-proto


## Motivation

Pulsar stores topics in bookkeeper ledgers, henceforth referred to as segments to avoid confusion with “ManagedLedger”. BookKeeper segments are normally replicated to three nodes. This can get expensive if you want to store the data for a long time. Even more so, if these are fast disks.

Bookkeeper segments have 3 primary access patterns.
 * Writes: Need to have low latency
 * Tailing reads: Need to have low latency
 * Catchup reads: Latency is unimportant. Throughput is important but how important depends on the use case.

BookKeeper uses the same type of storage for all three patterns. Moveover, bookkeeper uses the simplest form of replication available. It just makes 3(or N) copies and puts them on three different disks. The 3 copies are important while writing as it makes the voting protocol simple.

However, once a segment has been sealed it is immutable. I.e. The set of entries, the content of those entries and the order of the entries can never change. As a result once a segment is sealed you don’t need store it on your expensive 3X replicated SSD cluster. It can be transferred to some object store using Reed-Solomon on HDDs.

The remainder of this document covers how we will do this (in the context of Pulsar). 

## Background

Pulsar topics are stored in managed ledgers. Each topic (or partition of the topic, if the topic is partitioned), is stored on a single managed ledger. A managed ledger consists of a list of log segments, in a fixed order, oldest first. All segments in this list, apart from the most recent, are sealed. The most recent segment is the segment messages are currently being written to.

For tiered storage, the unit of storage will be a segment.

## Proposal

### High level

To implement tiered storage, Pulsar will copy segments as a whole from bookkeeper to an object store. Once the segment has been copied, we tag the segment in the managed ledger’s segment list with a key that identifies the segment in the object store. Once the tag has been added to the segment list, the segment can be deleted from bookkeeper.

For Pulsar to read back and serve messages from the object store, we will provide a ReadHandle implementation which reads data from the object store. With this, Pulsar only needs to know whether it is reading from bookkeeper or the object store when it is constructing the ReadHandle.

### Detail

Tiered storage requires the following changes:

 * An interface for offloading
 * Modifications to managed ledger to use offloading
 * Triggering mechanism
 * Offloading implementation for S3

#### LedgerOffloader interface

```java
interface LedgerOffloader {
    CompletableFuture<Void> offload(ReadHandle ledger,
                                    UUID uid,
                                    Map<String, String> extraMetadata);
    CompletableFuture<ReadHandle> readOffloaded(long ledgerId, UUID uid);
    CompletableFuture<Void> deleteOffloaded(long ledgerId, UUID uid);
}
```

The *offload* method should take a `ReadHandle` to a segment, and copy all the data in that segment to an object store. It also takes a UUID which identifies the attempt to offload the segment. This UUID is stored in the managed ledger metadata before the offload attempt, so that if a crash occurs, the failed attempt can be cleaned up by a later attempt. The UUID also prevents multiple concurrent attempts from interfering with each other, though this eventuallity should never occur.

The `extraMetadata` parameter is used to add some extra information to the object in the data store. For example, it can be used to carry the name of the managed ledger the segment is part of and the version of the software doing the offloading. The metadata is not intended to be read back by Pulsar, but rather to be used in inspection and debugging of the object store.

The `readOffloaded` method takes a ledger ID and the aforementioned UUID and returns a `ReadHandle` which is backed by an object in the object store. Reading from this handle is identical to reading from the bookkeeper-backed `ReadHandle` passed into offload.

The `deleteOffloaded` method deletes the segment from the object store.

A `LedgerOffloader` implementation is passed as a parameter in a `ManagedLedgerConfig` object when opening a managed ledger via the managed ledger factory. If no implementation is specified, a default implementation is used which simply throws an exception on any usage.

Segment offloading is implemented as an interface rather than directly in managed ledger to allow implementations to inspect and massage the data before it goes to the object store. For example, it may make sense for Pulsar to break out batch messages into individual messages before storing in the object store, so that the object can be used directly by some other system. Managed ledgers know nothing of these batches, nor should they, so the interface allows these concerns to be kept separate.


### Modifications to ManagedLedger

From the perspective of ManagedLedger, offloading looks very similar to trimming the head of the log, so it will follow the same pattern.

However, trimming occurs in the background, after a specified retention time, whereas offloading is triggered directly by the client. Offloading after a specified retention time will likely be added in a later iteration.

A new managed ledger call is provided.

```java
interface ManagedLedger {
    Position asyncOffloadPrefix(Position pos, OffloadCallback cb, Object ctx);
}
```

This call selects all sealed segments in the managed ledger before the passed in position, offloads them to an object store and updates the metadata. The original segments will be deleted after a configured lag time, 4 hours by default.

The metadata has a new context object for each segment in the managed ledger. This context contains:

- UUID the uuid of the most recent offload attempt
- A boolean flag to indicate whether offload has completed
- A boolean flag to indicate if the original bookkeeper segment has been deleted
- The timestamp when the offload succeeded

### Triggering an offload

There will be a admin rest endpoint on the broker, to which a managed ledger position can be passed, and this will call `asyncOffloadPrefix` on the managed ledger.

Offloading can also be enabled to occur automatically for all topics within a namespace. There are two new configurations on a namespace, `offloadTimeInMinutes` and `offloadSizeInMB` similar to the corresponding configurations for retention. 

Setting `offloadTimeInMinutes` will cause a segment to be offloaded to longterm storage when it has been sealed for that many minutes. This value must be lower than `retentionTimeInMinutes`, as otherwise the segment would be deleted before being offloaded.

Setting `offloadSizeInMB` will cause segments to be offloaded to longterm storage when the tail of the log reaches the specified size in MB. This value must be lower than `retentionSizeInMB`, as otherwise the segment would be deleted before being offloaded.

### S3 Implementation of SegmentOffloader

A single segment in bookkeeper maps to a two S3 objects, an index and a data object. The data object contains all the entries of the segment, while the index contains pointers into data object to allow quicker lookup of specific entries. The index entries are per block.

The data format version for both the index and data objects is stored as part of the S3 object user metadata.

The data object is written to S3 using a [multipart upload](https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadInitiate.html). The payload is split into 64MB blocks, and each block is uploaded as a “part”.

S3’s API requires that the size of the uploaded part is specified before sending the data. Therefore, all blocks apart from the final block must be exactly 64MB. Blocks can only contain complete entries, so if an entry doesn’t fit into the end of a block, the remaining bytes must be padded, with the pattern 0xFEDCDEAD.

The size of the final block can be calculated exactly before writing. The calculation is as follows.

```
(block header size)
 + ((length of segment) - (sum of length of already written entries))
 + ((number of entries to be written) * 4 * 8)
```

For each entry, 4 bytes are used to specify the length, and 8 bytes are used to store the entry ID.

#### Payload format

A block has a short header of length 128, followed by payload data.
```
[ magic_word ][ header_len ][ block_len ][ first_entry_id ][ padding ]
```

 * `magic_word` : 4 bytes, a sequence of bytes used to identify the start of a block
 * `header_len` : 8 bytes, the length of the block header (128 for now)
 * `block_len` : 8 bytes, the length of the block, including the header
 * `first_entry_id` : 8 bytes, Entry ID of first entry in the block
 * `padding` : As many bytes as necessary to bring the header length to the length specified by `header_len`

The payload data is a sequence of entries of the format

```
[ entry_len ][ entry_id ][ entry_data ]
```

 * `entry_len` : 4 bytes, the length of the entry
 * `entry_id` : 8 bytes, the ID of the entry
 * `entry_data` : as many bytes as specified in entry_len

Padding may be added at the end to bring the block to the correct size.

#### Index format

The index is a short header and then sequence of mappings.

The index header is
```
[ index_magic_word ][ index_len ][ data_object_length ][ data_header_length ][ index_entry_count ][ segment metadata ]
```

 * `index_magic_word` : 4 bytes, a sequence of bytes to identify the index
 * `index_len` : 4 bytes, the total length of the index in bytes, including the header
 * `data_object_length` : 8 bytes, the total length of the data object this index is associated with
 * `data_header_length` : 8 bytes, the length of the data block headers in the data object
 * `index_entry_count` : 4 bytes, the total number of mappings in the index
 * `segment_metadata_len` : 4 bytes, the length of the segment metadata
 * `segment_metadata` : the binary representation of the segment metadata, stored in protobuf format

The body of the index contains mappings from the entry id of the first entry in a block to the block id, and the offset of the block within the S3 object. For example:

```
[ entryId:   0 ] -> [ block: 1, offset: 0]
[ entryId: 100 ] -> [ block: 2, offset: 12345 ]
...
```

 * `entryId` is 8 bytes
 * `block` is 4 bytes
 * `offset` is 8 bytes

Block id corresponds to S3 object part number. For the payload they start at 2. The index is stored in part 1. The offset is the byte offset of block, from the start of the file.

The offset for a block can be calculated as

```
((number of preceeding blocks) * (block size, 64MB in this case))
```

#### Writing to S3

We use the S3 multipart API to upload a segment to S3. The first entry id of each block is recorded locally along with the part id and the length of the block.

```java
void writeToS3(ReadHandle segment) {
  var uploadId = initiateS3MultiPartUpload();

  var bytesWritten = 0;
  var nextEntry = 0;
  while (nextEntry <= segment.getLastAddConfirmed()) {
    var blockSize = calcBlockSizeForRemainingEntries(
            segment, bytesWritten, nextEntry);

    var is = new BlockAwareSegmentInputStream(
            segment, nextEntry, blockSize);
    var partId = pushPartToS3(uploadId, blockSize, is);

    addToIndex(firstEntry, partId, blockSize);

    nextEntry = is.lastEntryWritten() + 1;
    bytesWritten += is.entryBytesWritten();
  }
  finalizeUpload(uploadId);

  buildAndWriteIndex();
}
```

The algorithm has roughly the form of the pseudocode above. As S3 doesn’t seem to have an asynchronous API which gives you fine grained access to multipart, the upload should run in a dedicated threadpool.

The most important component when writing is `BlockAwareSegmentInputStream`. This allows the reader to read exactly `blockSize` bytes. It first returns the header, followed by complete entries from the segment in the format specified above. Once it can no longer writer a complete entry due to the `blockSize` limit, it outputs bytes the pattern `0xFEDCDEAD` until it reaches the limit. It tracks the ID of the last entry it has written as well as the sum of the cumulative entry bytes written, which can be used by the caller calculate the size of the next block.

#### Reading from S3

To read back from S3 we make a read request for the index object for the segment. Then when a client makes request to read entries, we do a binary search into the index to find which blocks needed to be loaded and request those blocks.

We don’t load the whole block each time while reading, as this would mean holding many 64MB blocks in memory when only a fraction of that is currently useful. Instead we read 1MB at a time from S3 using by setting the range on the get object request. To read an entries in the middle of a block, we read from the start of the block until we find that block. The offset of some of the read entries are cached and these are used to enhance the index, so we can quickly find these entries, or nearby entries.
