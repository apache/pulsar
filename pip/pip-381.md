# PIP-381: Handle large PositionInfo state

# Background knowledge

In case of KEY_SHARED subscription and out-of-order acknowledgments, 
the PositionInfo state can be persisted to preserve the state, 
with configurable maximum number of ranges to persist:

```
# Max number of "acknowledgment holes" that are going to be persistently stored.
# When acknowledging out of order, a consumer will leave holes that are supposed
# to be quickly filled by acking all the messages. The information of which
# messages are acknowledged is persisted by compressing in "ranges" of messages
# that were acknowledged. After the max number of ranges is reached, the information
# will only be tracked in memory and messages will be redelivered in case of
# crashes.
managedLedgerMaxUnackedRangesToPersist=10000
```

The PositionInfo state is stored to the BookKeeper as a single entry, and it can grow large if the number of ranges is large.
Currently, this means that BookKeeper can fail persisting too large PositionInfo state, e.g. over 1MB 
by default and the ManagedCursor recovery on topic reload might not succeed.

There is an abandoned PIP-81 for similar problem, this PIP takes over.

# Motivation

While keeping the number of ranges low to prevent such problems is a common sense solution, there are cases
where the higher number of ranges is required. For example, in case of the JMS protocol handler,
JMS consumers with filters may end up processing data out of order and/or at different speed,
and the number of ranges can grow large.

# Goals

Store the PositionInfo state in a BookKeeper ledger as multiple entries if the state grows too large to be stored as a single entry.

## In Scope

Transparent backwards compatibility if the PositionInfo state is small enough.

## Out of Scope

Backwards compatibility in case of the PositionInfo state is too large to be stored as a single entry.

# High Level Design

Cursor state writes and reads are happening at the same cases as currently, without changes.

Write path:

1. serialize the PositionInfo state to a byte array.
2. if the byte array is smaller than the threshold, store it as a single entry, as now. Done.
3. if the byte array is larger than the threshold, split it to smaller chunks and store the chunks in a BookKeeper ledger.
4. write the "footer" into the metadata store as a last entry.

See `persistPositionToLedger()` in `ManagedCursorImpl` for the implementation.

The footer is a JSON representation of

```java
    public static final class ChunkSequenceFooter {
        private int numParts;
        private int length;
    }
```

Read path:

1. read the last entry from the metadata store.
2. if the entry does not appear to be a JSON, treat it as serialized PositionInfo state and use it as is. Done.
3. if the footer is a JSON, parse number of chunks and length from the json. 
4. read the chunks from the BookKeeper ledger (entries from `startPos = footerPosition - chunkSequenceFooter.numParts` to `footerPosition - 1`) and merge them.
5. parse the merged byte array as a PositionInfo state.

See `recoverFromLedgerByEntryId()` in `ManagedCursorImpl` for the implementation.

## Design & Implementation Details

Proposed implementation: https://github.com/apache/pulsar/pull/22799

## Public-facing Changes

Nothing

### Public API

None

### Binary protocol

No public-facing changes

### Configuration

* **managedLedgerMaxUnackedRangesToPersist**: int, default 10000 (existing parameter). Controls number of unacked ranges to store.
* **persistentUnackedRangesWithMultipleEntriesEnabled**: boolean, default false. If true, the PositionInfo state is stored as multiple entries in BookKeeper if it grows too large.
* **persistentUnackedRangesMaxEntrySize**: int, default 1MB. Maximum size of a single entry in BookKeeper, in bytes.
* **cursorInfoCompressionType**: string, default "NONE". Compression type to use for the PositionInfo state.

### CLI

None

### Metrics

<!--
For each metric provide:
* Full name
* Description
* Attributes (labels)
* Unit
-->


# Monitoring

Existing monitoring should be sufficient.

# Security Considerations

N/A

# Backward & Forward Compatibility

## Upgrade

Not affected, just upgrade.

## Downgrade / Rollback

Not affected, just downgrade **as long as the managedLedgerMaxUnackedRangesToPersist was in the range to fit it into a single entry in BK**.

## Pulsar Geo-Replication Upgrade & Downgrade/Rollback Considerations

Not affected AFAIK.

# Alternatives

1. Do nothing. Keep the number of ranges low. This does not fit some use cases.
2. Come up with an extremely efficient storage format for the unacked ranges to fit them into a single entry all the time for e.g. 10mil ranges. This breaks backwards compatibility and the feasibility is unclear.

# General Notes

# Links

* Proposed implementation: https://github.com/apache/pulsar/pull/22799
* PIP-81: https://github.com/apache/pulsar/wiki/PIP-81:-Split-the-individual-acknowledgments-into-multiple-entries
* PR that implements better storage format for the unacked ranges (alternative 2): https://github.com/apache/pulsar/pull/9292

ML discussion and voting threads:

* Mailing List discussion thread: https://lists.apache.org/thread/8sm0h804v5914zowghrqxr92fp7c255d
* Mailing List voting thread: https://lists.apache.org/thread/q31fx0rox9tdt34xsmo1ol1l76q8vk99
