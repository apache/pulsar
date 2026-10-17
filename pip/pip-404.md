# PIP-404: Introduce per-ledger properties

# Background knowledge

As we don't have a secondary index on the Bookkeeper, so we can't query entries by message metadata efficiently.
The `ManagedCursor` provides a method `asyncFindNewestMatching` to find the newest entry that matches the given
predicate by binary search(See `OpFindNewest.java`).
In https://github.com/apache/pulsar/pull/22792, we optimized `seeking by timestamp` by calculating
the range of ledgers that may contain the target timestamp by `LedgerInfo#timestamp` and we don't need to scan all
ledgers.

However, when we enabled `AppendIndexMetadataInterceptor` and we want to query entries by `BrokerEntryMetadata#index`,
there is no more efficient way,
we have to scan all ledgers by binary search to find the target entry.

# Motivation

Introduce per-ledger properties and we can store the extra per-ledger properties in the `LedgerInfo`,
so we can query entries by `incremental index` more efficiently, say, `BrokerEntryMetadata#index`.

# Goals

## In Scope

* Provide a way to set per-ledger properties, it should be a generic way and can be used by any plugin.

## Out of Scope

* Set extra per-ledger properties, it should be done by the specific plugin. Such as `KoP`.

# High Level Design

We can store the `incremental index` of the first entry in the ledger into `LedgerInfo#properties`, say,
`BrokerEntryMetadata#index`.
When we want to query entries by `BrokerEntryMetadata#index`, we can calculate the range of ledgers that may contain the
target index by `LedgerInfo#properties` and we don't need to scan all ledgers.

In https://github.com/apache/pulsar/pull/22792, we provided a new method to find the newest entry with given range of entries:
```java
void asyncFindNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition,
                                 Position startPosition, Position endPosition, FindEntryCallback callback,
                                 Object ctx, boolean isFindFromLedger) {
    }
```
We can use this method directly in the above scenario.

# Detailed Design

## Public-facing Changes

### Public API

* Add the following method in `ManagedLedger`:

```java
    CompletableFuture<Void> asyncAddLedgerProperty(long ledgerId, String key, String value);
    CompletableFuture<Void> asyncRemoveLedgerProperty(long ledgerId, String key);
```

### Binary protocol

* Add a new field `properties` in `LedgerInfo`:

```protobuf
message LedgerInfo {
      required int64 ledgerId = 1;
      optional int64 entries = 2;
      optional int64 size = 3;
      optional int64 timestamp = 4;
      optional OffloadContext offloadContext = 5;
      // Add the following field
      repeated KeyValue properties = 6;
}
```

# Backward & Forward Compatibility

It is fully backward compatible.

# Alternatives

None

# Links

<!--
Updated afterwards
-->

* Mailing List discussion thread: https://lists.apache.org/thread/bcf13todophd05tn0qrrdwqyw8yvboly
* Mailing List voting thread: https://lists.apache.org/thread/dtj9ccntsjorb54yrb5fr3pppwl4m2r5
