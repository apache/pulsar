# PIP-81: Split the individual acknowledgments into multiple entries

- Status: [Discuss]
- Authors: Lin Lin, Penghui Li
- Release: 2.8.0
- Proposal time: 2021/01/04

# Motivation

Pulsar persists the individual acknowledgments and cumulative acknowledgments into a Ledger for each subscription. For the cumulative acknowledgments, a mark delete position is enough to represent the acknowledgment state, but for the individual acknowledgments, Pulsar needs to maintain a mark delete position to indicate the continuous acknowledgments state and non-continuous acknowledgments after the mark delete position.

More acknowledgment holes introduced more memory overhead and more storage overhead for maintaining the non-continuous acknowledgments. Currently, the acknowledgments state is organized by a series of entries. The broker always creates a new entry for the last acknowledgments state and append it to a Ledger and recover by reading the last entry. This will have the following disadvantages:

1. Too much acknowledgment holes will result in the entry over 5 MB, this will be refused to write to bookies by default.
2. The broker always loads all acknowledgment state into memory, this may exhaust the memory resources of the broker.
3. The broker always persists all acknowledgment state into the bookies but not all messages acknowledgment state changed, this may cause write magnification many times.

Currently, Pulsar support limits the serialized data size by setting `managedLedgerMaxUnackedRangesToPersist` at the `broker.conf`. By default, the broker persistent 10000 unacknowledged ranges to the bookies. But this will cause another problem, acknowledgment lost while recovering the acknowledgment state.

So, this proposal attempts to solve the above problems.





# Approach

The data of a Pulsar topic distributed in a series of ledgers and the message acknowledgment request always carried a ledger ID. So the core idea is splitting the acknowledgment state into multiple parts based on the size specified by users.

First, we will use multiple entries to save the message acknowledgment, and let each entry only save the data of one user ledger. Since it is divided into multiple entries, we need a marker to record which entries are the latest records. Considering that there are many cursors, zk is not suitable for storing too much data, so we save the marker as a special entry to the end. Every time all entries are recorded, a maker must be written at the same time. When we perform data recovery, read the entry from back to front.

![image](https://user-images.githubusercontent.com/12592133/108643140-24945b00-74e4-11eb-8cc6-2870e23b0b1a.png)

```
Description of some abnormal scenarios:

1. What happens if the broker fails to write the entry marker? For example, at t0, the broker flushes dirty pages and successfully writes an entry marker. At t1, the broker tries to flush dirty pages but fails to write the new entry marker. How can you recover the entry marker?
Now the persistence of ack is also asynchronous, not every ack will be persisted immediately, and the lost part will be re-consumed.
When the entry is successfully written but the marker fails to be written, we will consider the whole to be a failure and will continue to look
forward until we find a usable marker.
For the compatibility of old data, we will explain in the "Compatibility" chapter

2.  When a broker crashes and recovers the managed ledger, the cursor ledger is not writable anymore. Are you going to create a new cursor ledger and copy all the entries from the old cursor ledger to the new one?
We only copy the entry in the marker, other entries will not be copied. It's useless even if copied.
```

Then, we only load part of the data into the memory to solve the problem that may cause OOM when the amount of acknowledgment data is large. We need to add a parameter `maxIndividualAckCacheBytesPerCursor`. When the cursor is recovering, we start loading data into memory from last to earliest until the above limit is exceeded. If the acknowledgment is within the range of the marker, but the data is not loaded into memory, we swap in and out through LRU. When the LRU is swapped in and out, switch in the entry as the unit (each entry records the data of one user ledger), and swap in at least one ledger's acknowledgment data each time.
ps: It is possible that the acknowledgment data of a ledger exceeds maxEntrySize, which can be solved by configuring the maximum number of entries of the ledger. We can support ledger's ack data cross-entry in the future.

Finally, we solve the problem of saving the full acknowledgments every time by marking dirty pages. Because the marker preserves the relationship between entry and ledger. Therefore, we can maintain a Map to identify which ledger's acknowledgment has been updated, and the updated ledger is considered dirty, and the corresponding new entry will be saved. The marker can update the index of this entry.
Assuming that only the acknowledgment in entry1 is updated, only the index information of entry-1 in maker-2 needs to be updated to entry-N1 (marked in blue in the picture), and other records still use the previous entry2 to entryN.
As shown below:

![image](https://user-images.githubusercontent.com/12592133/108643161-3aa21b80-74e4-11eb-8e28-0e5734fdc3bc.png)

# Changes

## API Changes
1.add API 
`List<List<MLDataFormats.MessageRange>> buildIndividualDeletedMessageRanges()`
The returned results are stored in accordance with the ledger dimension, and each ledger has a list

2.add API `lruSwitch(long ledgerIdSwitchIn)`
When ledgerId is in the maker's index, but not stored in the memory, call `lruSwitch(long ledgerIdSwitchIn)` to switch acknowledgment data

## Protocol Changes
We will use the `properties` in `MLDataFormats.proto` to save the new marker identifier, so there is no need to modify the protocol

## Configuration Changes
Add parameter `maxIndividualAckCacheBytesPerCursor` to limit the maximum cache acknowledgment in memory. Default value set to 3MB(Less than the default maximum size of the entry).
`enableLruCacheMaxUnackedRanges` to enable and disabled this feature.



# Compatibility
When we use the new marker method, the written entry will be recorded with a special identifier. When we recovered, we found that if there is no special mark, we will recover in the old way.

# Test Plan
1.Save and restore acknowledgment normally
2.Can the index in maker be deleted correctly when `newMarkDeletePosition` changes
3.When `maxIndividualAckCacheBytesPerCursor` is less than the acknowledgement data of a ledger, can it be saved and restored correctly?
4.Normal LRU switching
5.When the LRU is switched, the data to be swapped in requires at least 2 ledgers to be swapped out
