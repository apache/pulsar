---
id: version-2.7.4-cookbooks-bookkeepermetadata
title: BookKeeper Ledger Metadata
original_id: cookbooks-bookkeepermetadata
---

Pulsar stores data on BookKeeper ledgers, you can understand the contents of a ledger by inspecting the metadata attached to the ledger.
Such metadata are stored on ZooKeeper and they are readable using BookKeeper APIs.

Description of current metadata:

| Scope  | Metadata name | Metadata value |
| ------------- | ------------- | ------------- |
| All ledgers  | application  | 'pulsar' |
| All ledgers  | component  | 'managed-ledger', 'schema', 'compacted-topic' |
| Managed ledgers | pulsar/managed-ledger | name of the ledger |
| Cursor | pulsar/cursor | name of the cursor |
| Compacted topic | pulsar/compactedTopic | name of the original topic |
| Compacted topic | pulsar/compactedTo | id of the last compacted message |


