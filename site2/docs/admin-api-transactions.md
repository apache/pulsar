---
id: admin-api-transactions
title: Manage transactions
sidebar_label: "Transactions"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````


:::tip

This page only shows **some frequently used operations**.

- For the latest and complete information about `Pulsar admin`, including commands, flags, descriptions, and more, see [Pulsar admin doc](/tools/pulsar-admin/)

- For the latest and complete information about `REST API`, including parameters, responses, samples, and more, see {@inject: rest:REST:/} API doc.

- For the latest and complete information about `Java admin API`, including classes, methods, descriptions, and more, see [Java admin API doc](/api/admin/).

:::

## Transaction resources

### GetSlowTransactions

In the production environment, there may be some long-lasting transactions that have never been completed. You can get these slow transactions that have survived over a certain time under a coordinator or all coordinators in the following ways.

````mdx-code-block
<Tabs groupId="api-choice"
 defaultValue="pulsar-admin"
 values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin transactions slow-transactions -c 1 -t 1s
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v3/transactions/slowTransactions/:timeout|operation/getSlowTransactions?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.transactions().getSlowTransactionsByCoordinatorId(coordinatorId, timeout, timeUnit)
//Or get slow transactions from all coordinators
admin.transactions().getSlowTransactions(timeout, timeUnit)
```

</TabItem>

</Tabs>
````

The following is an example of the returned values.

```json
{
  "(0,3)": {
    "txnId": "(0,3)",
    "status": "OPEN",
    "openTimestamp": 1658120122474,
    "timeoutAt": 300000,
    "producedPartitions": {},
    "ackedPartitions": {}
  },
  "(0,2)": {
    "txnId": "(0,2)",
    "status": "OPEN",
    "openTimestamp": 1658120122471,
    "timeoutAt": 300000,
    "producedPartitions": {},
    "ackedPartitions": {}
  },
  "(0,5)": {
    "txnId": "(0,5)",
    "status": "OPEN",
    "openTimestamp": 1658120122478,
    "timeoutAt": 300000,
    "producedPartitions": {},
    "ackedPartitions": {}
  },
  "(0,4)": {
    "txnId": "(0,4)",
    "status": "OPEN",
    "openTimestamp": 1658120122476,
    "timeoutAt": 300000,
    "producedPartitions": {},
    "ackedPartitions": {}
  },
  "(0,7)": {
    "txnId": "(0,7)",
    "status": "OPEN",
    "openTimestamp": 1658120122482,
    "timeoutAt": 300000,
    "producedPartitions": {},
    "ackedPartitions": {}
  },
  "(0,10)": {
    "txnId": "(0,10)",
    "status": "OPEN",
    "openTimestamp": 1658120122488,
    "timeoutAt": 300000,
    "producedPartitions": {},
    "ackedPartitions": {}
  },
  "(0,6)": {
    "txnId": "(0,6)",
    "status": "OPEN",
    "openTimestamp": 1658120122480,
    "timeoutAt": 300000,
    "producedPartitions": {},
    "ackedPartitions": {}
  },
  "(0,9)": {
    "txnId": "(0,9)",
    "status": "OPEN",
    "openTimestamp": 1658120122486,
    "timeoutAt": 300000,
    "producedPartitions": {},
    "ackedPartitions": {}
  },
  "(0,8)": {
    "txnId": "(0,8)",
    "status": "OPEN",
    "openTimestamp": 1658120122484,
    "timeoutAt": 300000,
    "producedPartitions": {},
    "ackedPartitions": {}
  },
  "(0,11)": {
    "txnId": "(0,11)",
    "status": "OPEN",
    "openTimestamp": 1658120122490,
    "timeoutAt": 300000,
    "producedPartitions": {},
    "ackedPartitions": {}
  }
}
```

### ScaleTransactionCoordinators

When the performance of transactions reaches a bottleneck due to the insufficient number of transaction coordinators, you can scale the number of the transaction coordinators in the following ways.

````mdx-code-block
<Tabs groupId="api-choice"
 defaultValue="pulsar-admin"
 values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin transactions scale-transactionCoordinators -r 17
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v3/transactions/transactionCoordinator/:replicas|operation/scaleTransactionCoordinators?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.transactions().scaleTransactionCoordinators(replicas);
```

</TabItem>

</Tabs>
````

## Transaction stats

### Get transaction metadata

The transaction metadata that can be retrieved include:
* **txnId:** The ID of this transaction.
* **status:** The status of this transaction.
* **openTimestamp:** The open time of this transaction.
* **timeoutAt:** The timeout of this transaction.
* **producedPartitions:** The partitions or topics that messages have been sent to with this transaction.
* **ackedPartitions:** The partitions or topics where messages have been acknowledged with this transaction.

Use one of the following ways to get your transaction metadata.

````mdx-code-block
<Tabs groupId="api-choice"
 defaultValue="pulsar-admin"
 values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin transactions transaction-metadata -m 1 -l 1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v3/transactions/transactionMetadata/:mostSigBits/:leastSigBits|operation/getTransactionMetadata?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.transactions().getTransactionMetadata(txnID);
```

</TabItem>

</Tabs>
````

The following is an example of the returned values.

```json
{
 "txnId" : "(1,18)",
 "status" : "ABORTING",
 "openTimestamp" : 1656592983374,
 "timeoutAt" : 5000,
 "producedPartitions" : {
   "my-topic" : {
     "startPosition" : "127:4959",
     "aborted" : true
   }
 },
 "ackedPartitions" : {
   "my-topic" : {
     "mysubName" : {
       "cumulativeAckPosition" : null
     }
   }
 }
}
```

### Get transaction stats in transaction pending ack

The transaction stats in transaction pending ack that can be retrieved include:
* **cumulativeAckPosition:** The position that this transaction cumulatively acknowledges in this subscription.

Use one of the following ways to get transaction stats in pending ack:

````mdx-code-block
<Tabs groupId="api-choice"
 defaultValue="pulsar-admin"
 values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin transactions transaction-in-pending-ack-stats -m 1 -l 1 -t my-topic -s mysubname
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v3/transactions/transactionInPendingAckStats/:tenant/:namespace/:topic/:subName/:mostSigBits/:leastSigBits|operation/getTransactionInPendingAckStats?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.transactions().getTransactionInPendingAckStats(txnID, topic, subname);
```

</TabItem>

</Tabs>
````

The following is an example of the returned value.

```json
{
 "cumulativeAckPosition" : "137:49959"
 }
```

### Get transaction stats in transaction buffer

The transaction stats in the transaction buffer that can be retrieved include:
* **startPosition:** The start position of this transaction in the transaction buffer.
* **aborted:** The flag of whether this transaction has been aborted.

Use one of the following ways to get transaction stats in transaction buffer:

````mdx-code-block
<Tabs groupId="api-choice"
 defaultValue="pulsar-admin"
 values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin transactions transaction-in-buffer-stats -m 1 -l 1 -t my-topic
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v3/transactions/transactionInBufferStats/:tenant/:namespace/:topic/:mostSigBits/:leastSigBits|operation/getTransactionInBufferStats?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.transactions().getTransactionInBufferStatsAsync(txnID, topic);
```

</TabItem>

</Tabs>
````

The following is an example of the returned values.

```json
{
 "startPosition" : "137:49759",
 "aborted" : false
}
```

## Transaction coordinator stats

The transaction coordinator (TC) is a module inside a Pulsar broker. It maintains the entire life cycle of transactions and handles transaction timeout.

### Get coordinator stats
The transaction coordinator stats that can be retrieved include:
* **state:**  The state of this transaction coordinator.
* **leastSigBit:s** The sequence ID of this transaction coordinator.
* **lowWaterMark:** The low watermark of this transaction coordinator.
* **ongoingTxnSize:** The total number of ongoing transactions in this transaction coordinator.
* **recoverStartTime:** The start timestamp of transaction coordinator recovery. `0L` means no startup.
* **recoverEndTime:** The end timestamp of transaction coordinator recovery. `0L` means no startup.

Use one of the following ways to get transaction coordinator stats:

````mdx-code-block
<Tabs groupId="api-choice"
 defaultValue="pulsar-admin"
 values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin transactions coordinator-stats -c 1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v3/transactions/coordinatorStats|operation/getCoordinatorStats?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.transactions().getCoordinatorStatsById(coordinatorId);
//Or get all coordinator stats.
admin.transactions().getCoordinatorStats();
```

</TabItem>

</Tabs>
````

The following is an example of the returned values.

```json
{
  "state" : "Ready",
  "leastSigBits" : 1,
  "lowWaterMark" : 0,
  "ongoingTxnSize" : 0,
  "recoverStartTime" : 1657021892377,
  "recoverEndTime" : 1657021892378
}
```

### Get coordinator internal stats

The coordinator's internal stats that can be retrieved include:
* **transactionLogStats:** The stats of the transaction coordinator log.
* **managedLedgerName:** The name of the managed ledger where the transaction coordinator log is stored. 
* **managedLedgerInternalStats:** The internal stats of the managed ledger where the transaction coordinator log is stored. See `[managedLedgerInternalStats](admin-api-topics.md#get-internal-stats)` for more details.

Use one of the following ways to get coordinator’s internal stats:
````mdx-code-block
<Tabs groupId="api-choice"
 defaultValue="pulsar-admin"
 values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin transactions coordinator-internal-stats -c 1 -m
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v3/transactions/coordinatorInternalStats/:coordinatorId|operation/getCoordinatorInternalStats?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.transactions().getCoordinatorInternalStats(coordinatorId, metadata);
```

</TabItem>

</Tabs>
````

The following is an example of the returned values.

```json
{
 "transactionLogStats" : {
   "managedLedgerName" : "pulsar/system/persistent/__transaction_log_1",
   "managedLedgerInternalStats" : {
     "entriesAddedCounter" : 3,
     "numberOfEntries" : 3,
     "totalSize" : 63,
     "currentLedgerEntries" : 3,
     "currentLedgerSize" : 63,
     "lastLedgerCreatedTimestamp" : "2022-06-30T18:18:05.88+08:00",
     "waitingCursorsCount" : 0,
     "pendingAddEntriesCount" : 0,
     "lastConfirmedEntry" : "13:2",
     "state" : "LedgerOpened",
     "ledgers" : [ {
       "ledgerId" : 13,
       "entries" : 0,
       "size" : 0,
       "offloaded" : false,
       "metadata" : "LedgerMetadata{formatVersion=3, ensembleSize=1, writeQuorumSize=1, ackQuorumSize=1, state=CLOSED, length=63, lastEntryId=2, digestType=CRC32C, password=OMITTED, ensembles={0=[10.20.240.119:3181]}, customMetadata={component=base64:bWFuYWdlZC1sZWRnZXI=, pulsar/managed-ledger=base64:cHVsc2FyL3N5c3RlbS9wZXJzaXN0ZW50L19fdHJhbnNhY3Rpb25fbG9nXzE=, application=base64:cHVsc2Fy}}",
       "underReplicated" : false
     } ],
     "cursors" : {
       "transaction.subscription" : {
         "markDeletePosition" : "13:2",
         "readPosition" : "13:3",
         "waitingReadOp" : false,
         "pendingReadOps" : 0,
         "messagesConsumedCounter" : 3,
         "cursorLedger" : 22,
         "cursorLedgerLastEntry" : 1,
         "individuallyDeletedMessages" : "[]",
         "lastLedgerSwitchTimestamp" : "2022-06-30T18:18:05.932+08:00",
         "state" : "Open",
         "numberOfEntriesSinceFirstNotAckedMessage" : 1,
         "totalNonContiguousDeletedMessagesRange" : 0,
         "subscriptionHavePendingRead" : false,
         "subscriptionHavePendingReplayRead" : false,
         "properties" : { }
       }
     }
   }
 }
}
```

## Transaction pending ack stats

Pending ack maintains message acknowledgments within a transaction before a transaction completes. If a message is in the pending acknowledge state, the message cannot be acknowledged by other transactions until the message is removed from the pending acknowledge state.

### Get transaction pending ack stats

The transaction pending ack state stats that can be retrieved include:
* **state:** The state of this transaction coordinator.
* **lowWaterMark:** The low watermark of this transaction coordinator.
* **ongoingTxnSize:** The total number of ongoing transactions in this transaction coordinator.
* **recoverStartTime:** The start timestamp of transaction pendingAck recovery. `0L` means no startup.
* **recoverEndTime:** The end timestamp of transaction pendingAck recovery. `0L` means no startup.

Use one of the following ways to get transaction pending ack stats:

````mdx-code-block
<Tabs groupId="api-choice"
 defaultValue="pulsar-admin"
 values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin.transactions()s pending-ack-stats -t my-topic -s mysubName -l
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v3/transactions/pendingAckStats/:tenant/:namespace:/:topic:/:subName|operation/getPendingAckStats?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.transactions().getPendingAckStats(topic, subName, lowWaterMarks)
```

</TabItem>

</Tabs>
````

The following is an example of the returned values.

```json
{
  "state" : "Ready",
  "lowWaterMarks" : {
    "1" : 0
  },
  "ongoingTxnSize" : 1,
  "recoverStartTime" : 1657021899202,
  "recoverEndTime" : 1657021899203
}
```

### Get transaction pending ack internal stats

The transaction pending ack internal stats that can be retrieved include:
* **transactionLogStats:** The stats of the transaction pending ack log.
* **managedLedgerName:** The name of the managed ledger where the transaction pending ack log is stored. 
* **managedLedgerInternalStats:** The internal stats of the managed ledger where the transaction coordinator log is stored. See `[managedLedgerInternalStats](admin-api-topics.md#get-internal-stats)` for more details.

Use one of the following ways to get transaction pending ack internal stats:

````mdx-code-block
<Tabs groupId="api-choice"
 defaultValue="pulsar-admin"
 values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin transactions pending-ack-internal-stats -t my-topic -s mysubName -m
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v3/transactions/pendingAckInternalStats/:tenant/:namespace:/:topic:/:subName|operation/getPendingAckInternalStats?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.transactions().getPendingAckInternalStats(topic, subName, boolean metadata);
```

</TabItem>

</Tabs>
````

The following is an example of the returned values.

```json
{
 "pendingAckLogStats" : {
   "managedLedgerName" : "public/default/persistent/my-topic-mysubName__transaction_pending_ack",
   "managedLedgerInternalStats" : {
     "entriesAddedCounter" : 2247,
     "numberOfEntries" : 2247,
     "totalSize" : 37212,
     "currentLedgerEntries" : 104,
     "currentLedgerSize" : 1732,
     "lastLedgerCreatedTimestamp" : "2022-06-30T19:02:09.746+08:00",
     "waitingCursorsCount" : 0,
     "pendingAddEntriesCount" : 52,
     "lastConfirmedEntry" : "64:51",
     "state" : "LedgerOpened",
     "ledgers" : [ {
       "ledgerId" : 56,
       "entries" : 2195,
       "size" : 36346,
       "offloaded" : false,
       "metadata" : "LedgerMetadata{formatVersion=3, ensembleSize=1, writeQuorumSize=1, ackQuorumSize=1, state=CLOSED, length=36346, lastEntryId=2194, digestType=CRC32C, password=OMITTED, ensembles={0=[10.20.240.119:3181]}, customMetadata={component=base64:bWFuYWdlZC1sZWRnZXI=, pulsar/managed-ledger=base64:cHVibGljL2RlZmF1bHQvcGVyc2lzdGVudC9teS10b3BpYy1teXN1Yk5hbWVfX3RyYW5zYWN0aW9uX3BlbmRpbmdfYWNr, application=base64:cHVsc2Fy}}",
       "underReplicated" : false
     }, {
       "ledgerId" : 64,
       "entries" : 0,
       "size" : 0,
       "offloaded" : false,
       "metadata" : "LedgerMetadata{formatVersion=3, ensembleSize=1, writeQuorumSize=1, ackQuorumSize=1, state=CLOSED, length=866, lastEntryId=51, digestType=CRC32C, password=OMITTED, ensembles={0=[10.20.240.119:3181]}, customMetadata={component=base64:bWFuYWdlZC1sZWRnZXI=, pulsar/managed-ledger=base64:cHVibGljL2RlZmF1bHQvcGVyc2lzdGVudC9teS10b3BpYy1teXN1Yk5hbWVfX3RyYW5zYWN0aW9uX3BlbmRpbmdfYWNr, application=base64:cHVsc2Fy}}",
       "underReplicated" : false
     } ],
     "cursors" : {
       "__pending_ack_state" : {
         "markDeletePosition" : "56:-1",
         "readPosition" : "56:0",
         "waitingReadOp" : false,
         "pendingReadOps" : 0,
         "messagesConsumedCounter" : 0,
         "cursorLedger" : 57,
         "cursorLedgerLastEntry" : 0,
         "individuallyDeletedMessages" : "[]",
         "lastLedgerSwitchTimestamp" : "2022-06-30T18:55:26.842+08:00",
         "state" : "Open",
         "numberOfEntriesSinceFirstNotAckedMessage" : 1,
         "totalNonContiguousDeletedMessagesRange" : 0,
         "subscriptionHavePendingRead" : false,
         "subscriptionHavePendingReplayRead" : false,
         "properties" : { }
       }
     }
   }
 }
}
```

### Get position stats in pending ack

The position stats in pending ack include:
* **PendingAck** The position is in pending ack stats.
* **MarkDelete** The position is already acknowledged.
* **NotInPendingAck** The position is not acknowledged within a transaction.
* **PendingAckNotReady** The pending ack has not been initialized.
* **InvalidPosition** The position is invalid, for example, batch index > batch size.

If you want to know whether the position has been acknowledged, you can use one of the following ways to get position stats pending ack:

````mdx-code-block
<Tabs groupId="api-choice"
 defaultValue="pulsar-admin"
 values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin transactions position-stats-in-pending-ack -t my-topic -s mysubName -l 15 -e 6
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v3/transactions/pendingAckStats
/:tenant/:namespace:/:topic:/:subName/:ledgerId/:entryId?batchIndex=batchIndex|operation/getPositionStatsInPendingAck?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.transactions().getPositionStatsInPendingAckAsync(topic, subName, ledgerId, entryId, lowWaterMarks);
```

</TabItem>

</Tabs>
````

The following is an example of the returned values.

```json
{
  "State" : "MarkDelete"
}
```

## Transaction buffer stats

Transaction buffer handles messages produced to a topic partition within a transaction. 
The messages in the transaction buffer are not visible to consumers until the transactions are committed. The messages in the transaction buffer are discarded when the transactions are aborted. 

### Get transaction buffer stats

The transaction buffer stats that can be retrieved include:
* **state:** The state of this transaction buffer.
* **maxReadPosition:** The maximum read position of this transaction buffer.
* **lastSnapshotTimestamps:** The last snapshot timestamp of this transaction buffer.
* **lowWaterMarks (Optional):** The low watermark details of the transaction buffer.
* **ongoingTxnSize:** The total number of ongoing transactions in this transaction buffer.
* **recoverStartTime:** The start timestamp of transaction buffer recovery. `0L` means no startup.
* **recoverEndTime:** The end timestamp of transaction buffer recovery. `0L` means no startup.

Use one of the following ways to get transaction buffer stats:

````mdx-code-block
<Tabs groupId="api-choice"
 defaultValue="pulsar-admin"
 values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin transactions transaction-buffer-stats -t my-topic -l
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v3/transactions/transactionBufferStats/:tenant/:namespace:/:topic:/:subName|operation/getTransactionBufferStats?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.transactions().getTransactionBufferStats(topic, lowWaterMarks);
```

</TabItem>

</Tabs>
````

The following is an example of the returned values.

```json
{
  "state" : "Ready",
  "maxReadPosition" : "38:101",
  "lastSnapshotTimestamps" : 1657021903534,
  "lowWaterMarks" : {
    "1" : -1,
    "2" : -1
  },
  "ongoingTxnSize" : 0,
  "recoverStartTime" : 1657021892850,
  "recoverEndTime" : 1657021893372
}
```