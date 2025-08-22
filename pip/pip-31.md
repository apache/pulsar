# PIP-31: Transaction Support

* **Status**: Implemented
* **Author**: Sijie Guo
* **Pull Request**: https://github.com/apache/pulsar/pull/2664
* **Mailing List discussion**:
* **Release**: 2.8.0

[**Motivation**](#motivation)	**[4](#motivation)**

[Use Cases](#use-cases)	[4](#use-cases)

[Event Processing](#event-processing)	[4](#event-processing)

[Atomic Produce](#atomic-produce)	[5](#atomic-produce)

[Transactional Guarantees](#transactional-guarantees)	[**6**](#transactional-guarantees)

[Isolation Levels](#isolation-levels)	[6](#isolation-levels)

[A 10,000 Foot View](#a-10,000-foot-view)	[**7**](#a-10,000-foot-view)

[Concepts](#concepts)	[7](#concepts)

[Transaction Coordinator](#transaction-coordinator)	[7](#transaction-coordinator)

[Transaction Buffer](#transaction-buffer)	[7](#transaction-buffer)

[Acknowledgements in Transactions](#acknowledgements-in-transactions)	[8](#acknowledgements-in-transactions)

[Materialization Mechanism](#materialization-mechanism)	[8](#materialization-mechanism)

[Transaction Flow](#transaction-flow)	[9](#transaction-flow)

[1\. Begin Transaction](#1.-begin-transaction)	[10](#1.-begin-transaction)

[2\. Transaction Loop](#2.-transaction-loop)	[10](#2.-transaction-loop)

[2.1 Add Partitions To Txn](#2.1-add-partitions-to-txn)	[10](#2.1-add-partitions-to-txn)

[2.2 Produce Messages to Partitions](#2.2-produce-messages-to-partitions)	[10](#2.2-produce-messages-to-partitions)

[2.3 Add Subscriptions To Txn](#2.3-add-subscriptions-to-txn)	[11](#2.3-add-subscriptions-to-txn)

[2.4 Ack Messages on Subscriptions](#2.4-ack-messages-on-subscriptions)	[11](#2.4-ack-messages-on-subscriptions)

[3\. End Transaction](#3.-end-transaction)	[11](#3.-end-transaction)

[3.1 End Transaction Request](#3.1-end-transaction-request)	[11](#3.1-end-transaction-request)

[3.2 Finalizing Process](#3.2-finalizing-process)	[12](#3.2-finalizing-process)

[3.3 Mark Transaction as COMMITTED or ABORTED](#3.3-mark-transaction-as-committed-or-aborted)	[12](#3.3-mark-transaction-as-committed-or-aborted)

[**Design Choices**](#design-choices)	**[13](#design-choices)**

[Transaction Buffer](#transaction-buffer-1)	[13](#transaction-buffer-1)

[Marker Approach](#marker-approach)	[13](#marker-approach)

[Overview](#overview)	[14](#overview)

[Challenges](#challenges)	[15](#challenges)

[Materialization](#materialization)	[15](#materialization)

[Sweep](#sweep)	[15](#sweep)

[Retention](#retention)	[16](#retention)

[Changes](#changes)	[16](#changes)

[Discussions](#discussions)	[16](#discussions)

[Sidecar Approach](#sidecar-approach)	[17](#sidecar-approach)

[Overview](#overview-1)	[17](#overview-1)

[Challenges](#challenges-1)	[19](#challenges-1)

[Materialization](#materialization-1)	[19](#materialization-1)

[Sweep](#sweep-1)	[20](#sweep-1)

[Retention](#retention-1)	[20](#retention-1)

[Changes](#changes-1)	[20](#changes-1)

[Discussions](#discussions-1)	[20](#discussions-1)

[Huge Size Transaction](#huge-size-transaction)	[20](#huge-size-transaction)

[**A Full Proposal**](#a-full-proposal)	**[21](#a-full-proposal)**

[Public APIs](#public-apis)	[21](#public-apis)

[Examples](#examples)	[24](#examples)

[Data Structures](#data-structures)	[25](#data-structures)

[TxnID](#txnid)	[25](#txnid)

[Wire Protocol Changes](#wire-protocol-changes)	[25](#wire-protocol-changes)

[New Commands](#new-commands)	[25](#new-commands)

[TxnAction](#txnaction)	[25](#txnaction)

[CommandNewTxn](#commandnewtxn)	[25](#commandnewtxn)

[CommandNewTxnResponse](#commandnewtxnresponse)	[26](#commandnewtxnresponse)

[CommandAddPartitionToTxn](#commandaddpartitiontotxn)	[26](#commandaddpartitiontotxn)

[CommandAddPartitionToTxnResponse](#commandaddpartitiontotxnresponse)	[26](#commandaddpartitiontotxnresponse)

[CommandAddSubscritpionToTxn](#commandaddsubscritpiontotxn)	[27](#commandaddsubscritpiontotxn)

[CommandAddSubscriptionToTxnResponse](#commandaddsubscriptiontotxnresponse)	[27](#commandaddsubscriptiontotxnresponse)

[CommandEndTxn](#commandendtxn)	[28](#commandendtxn)

[CommandEndTxnResponse](#commandendtxnresponse)	[28](#commandendtxnresponse)

[CommandEndTxnOnPartition](#commandendtxnonpartition)	[28](#commandendtxnonpartition)

[CommandEndTxnOnPartitionResponse](#commandendtxnonpartitionresponse)	[29](#commandendtxnonpartitionresponse)

[CommandEndTxnOnSubscription](#commandendtxnonsubscription)	[29](#commandendtxnonsubscription)

[CommandEndTxnOnSubscriptionResponse](#commandendtxnonsubscriptionresponse)	[29](#commandendtxnonsubscriptionresponse)

[Modified Commands](#modified-commands)	[30](#modified-commands)

[CommandSend](#commandsend)	[30](#commandsend)

[CommandAck](#commandack)	[30](#commandack)

[Transaction Coordinator](#transaction-coordinator-1)	[31](#transaction-coordinator-1)

[Concepts](#concepts-1)	[33](#concepts-1)

[TCID](#tcid)	[33](#tcid)

[TxnStatus](#txnstatus)	[33](#txnstatus)

[TransactionMetadataStoreProvider](#transactionmetadatastoreprovider)	[33](#transactionmetadatastoreprovider)

[TransactionMetadataStore](#transactionmetadatastore)	[34](#transactionmetadatastore)

[Implementation](#implementation)	[34](#implementation)

[Transaction Log](#transaction-log)	[34](#transaction-log)

[TC Startup](#tc-startup)	[35](#tc-startup)

[TC Request Processing](#tc-request-processing)	[36](#tc-request-processing)

[CommandNewTxn](#commandnewtxn-1)	[36](#commandnewtxn-1)

[CommandAddPartitionToTxn](#commandaddpartitiontotxn-1)	[36](#commandaddpartitiontotxn-1)

[CommandAddSubscriptionToTxn](#commandaddsubscriptiontotxn)	[37](#commandaddsubscriptiontotxn)

[CommandEndTxn](#commandendtxn-1)	[37](#commandendtxn-1)

[TC Transaction Expiration](#tc-transaction-expiration)	[38](#tc-transaction-expiration)

[Deployment](#deployment)	[38](#deployment)

[TC in Broker](#tc-in-broker)	[38](#tc-in-broker)

[Broker \- Transaction Buffer](#broker---transaction-buffer)	[39](#broker---transaction-buffer)

[Message](#message)	[39](#message)

[Message ID](#message-id)	[39](#message-id)

[Transaction Log](#transaction-log-1)	[40](#transaction-log-1)

[Implementation](#implementation-1)	[40](#implementation-1)

[Transaction Manager](#transaction-manager)	[40](#transaction-manager)

[Dedup Cursor](#dedup-cursor)	[40](#dedup-cursor)

[Transaction Cursor](#transaction-cursor)	[40](#transaction-cursor)

[Transaction Index](#transaction-index)	[40](#transaction-index)

[Retention Cursor](#retention-cursor)	[41](#retention-cursor)

[Compacting Transaction Log](#compacting-transaction-log)	[41](#compacting-transaction-log)

[Partition Loading](#partition-loading)	[42](#partition-loading)

[Broker Request Processing](#broker-request-processing)	[42](#broker-request-processing)

[CommandSend](#commandsend-1)	[42](#commandsend-1)

[CommandEndTxnOnPartition](#commandendtxnonpartition-1)	[42](#commandendtxnonpartition-1)

[Broker Dispatching](#broker-dispatching)	[43](#broker-dispatching)

[Broker \- Acknowledgements](#broker---acknowledgements)	[43](#broker---acknowledgements)

[New Acknowledgement State](#new-acknowledgement-state)	[44](#new-acknowledgement-state)

[PENDING\_ACK State](#pending_ack-state)	[45](#pending_ack-state)

[Negative Acknowledgement](#negative-acknowledgement)	[46](#negative-acknowledgement)

[Client-Broker Request Processing](#client-broker-request-processing)	[46](#client-broker-request-processing)

[**Future Worker**](#future-worker)	**[46](#future-worker)**

[Single Partition Transaction](#single-partition-transaction)	[46](#single-partition-transaction)

# 

# PIP-31: Transactional Streaming

# Motivation {#motivation}

This document outlines a proposal for supporting transactional messaging at Apache Pulsar. Transactions are used for strengthening the message delivery semantics of Apache Pulsar and processing guarantees at Pulsar Functions.

The highest message delivery guarantee that Apache Pulsar currently provides is \`exactly-once\` producing at one single partition via Idempotent Producer. Users are guaranteed that every message produced to one single partition via an Idempotent Producer will be persisted exactly once, without data loss. There is no \`atomicity\` when a producer attempts to produce messages to multiple partitions. For instance, a publish failure can occur when the broker crashes and if the producer doesn’t retry or has exhausted its retry count, the message might not be written to Pulsar. On the consumer side, acknowledgement currently is a best-effort operation, which will result in message redelivery, hence consumers will receive duplicate messages. Pulsar only guarantees at-least-once consumption for consumers.

Similarly, Pulsar Functions only guarantees exactly-once processing on a single event on idempotent function. It can’t guarantee processing multiple events or producing multiple results can happen exactly once. For example, if a function accepts multiple events and produces one result (e.g. windowing functions), the function can fail between producing the result and acknowledging the incoming messages, or even between acknowledging individual events. This will cause all (or some) incoming messages being re-delivered and reprocessed, and a new result is generated.

Users of Pulsar and Pulsar Functions will greatly benefit from transactional semantic support. Every message written or processed will be happening exactly once, without duplicates and without data loss \- even in the event of broker or function instance failures. A transactional messaging semantic not only makes writing applications using Pulsar or Pulsar Functions easier, it expands the scope which Pulsar can provide.

## Use Cases {#use-cases}

### Event Processing {#event-processing}

Event Processing (aka Event Streaming, Stream Processing) applications will hugely benefit from transactional guarantees. Event processing application, which is typically a pipeline of ‘consume-transform-produce’ tasks, requires transactional guarantees when duplicate processing of the event stream is unacceptable. 

Pulsar Functions is one of such event processing framework built for Apache Pulsar. A function invocation is a typical \`consume-transform-produce\` task \- It reads events from one (or multiple) topic(s), processes the events by invoking the function, and produces the results to one (or multiple) topic(s).  [^1][^2]

Many SPE (stream processing engines) or customized event processing logic (using Pulsar producer and consumer) falls into the category of \`consume-transform-produce\`. It requires the ability to produce a batch of messages to different topic-partitions and acknowledge a batch of messages in one atomic operation. By atomically, we mean either all messages are committed (all output messages are persisted exactly-once and all input messages are acknowledged), or none of them are.

The number of messages produced and number of messages acknowledged in a single transaction can vary \- from a few messages or a ton of messages. The number of topic-partitions (aka partitions)[^3] can vary \- from one single partition to many partitions. It depends on the processing efficiency, windowing/buffering logic and many other application related factors. A typical example for understanding the variants described above is windowing operators in an event processing application. As the reader works through this document, we encourage him/her to keep this category of use cases in mind as it will motivate many design choices in the remaining part of this proposal.

### Atomic Produce {#atomic-produce}

Atomically producing multiple messages is a variant of \`consume-transfer-produce\`, where it only requires \`produce\` messages in an atomic way. 

Another variant of atomic-produce is for going beyond the 5MB message size limitation in Apache Pulsar. You can increase the limit of message to a very large message size, but it means you will have very large network and disk IO, which is not friendly to memory and disk utilization. People will end up breaking down a large message into multiple smaller chunks, and send each chunk as one pulsar message. If doing so, we need to make sure producing such chunks are done in an atomic way.

Database CDC or using Apache Pulsar as commit log are typical use cases for atomic producing.

# Transactional Guarantees {#transactional-guarantees}

As described in [Motivation](#bookmark=id.g2xdepks9p63) section, providing transactional semantics will enable event streaming applications to consume, process, and produce messages in one atomic operation.

That means, ***a batch of messages in a transaction can be received from, produced to and acknowledged to many partitions. All the operations involved in a transaction will succeed or fail as one single unit.*** 

However, we are not guaranteed the messages produced within a committed transaction will be consumed by its downstream consumers all together. This is for several reasons:

1. Consumers may not consume from all the partitions which participated in the committed transaction. Hence they will never be able to read all the messages that comprised in the transaction.  
2. Consumers may have a different receiver queue size or buffering/window size, which they might only be interested in consuming a certain amount of messages. That amount can be any arbitrary number.

*However, we might be able to support consuming messages committed to one single partition all together. But it depends on the design we choose below. As there is no strong requirement for this feature, I will leave this point outside of guarantees for now.*

## Isolation Levels {#isolation-levels}

Similar as Database Transactions, Transactions in an event streaming system will also have isolation levels. The isolations levels are:

- READ\_UNCOMMITTED: It means no isolation and consumers are able to read uncommitted messages.  
- READ\_COMMITTED: All consumers are only able to read committed messages.  
- SERIALIZABILITY:  The execution of multiple transactions over multiple partitions is equivalent to some serial execution (total ordering) of the transactions. This guarantees the committed messages of transaction A appear before the committed messages of transaction B if transaction A is committed before B. Hence, the consumer will be able to see exactly the same order of the committed messages across the partitions involved in those transactions.

The isolation level that Pulsar transaction must support is **READ\_COMMITTED**. Whether to support READ\_UNCOMMITTED or SERIALIZABILITY requires inputs from Pulsar users. 

# A 10,000 Foot View  {#a-10,000-foot-view}

There are many ways to design the transaction support in Pulsar. All these proposals can be shaped into one common framework, which we will discuss in this section. Further sections will describe the details based on this framework in detail.

## Concepts {#concepts}

### Transaction Coordinator {#transaction-coordinator}

In order to achieve transactional messaging, we will have to introduce a server-side module named ***Transaction Coordinator*** (aka TC). TC manages transactions of messages sent by producers and acknowledgements sent by consumers, and commit or abort the operations as a whole. 

The Transaction Coordinator will be persisting transaction statuses in some persistent storage (e.g. a transaction log backed by a separated topic, or a table in table service) for recovery. We will talk about how a TC can be implemented and how it manages the transaction statuses in further section.

### Transaction Buffer {#transaction-buffer}

Messages produced within a transaction will be stored  in ***Transaction Buffer*** (aka TB). The messages in TB will not be materialized (visible) to consumers until their transactions are committed. The messages in TB will be discarded when their transactions are aborted. A cleanup process (e.g. compaction) might be required to clean up messages of aborted transactions depending on how TB is implemented.

A TB implementation requires :

- Messages produced to TB are NOT duplicated no matter how producer retries producing messages.  
- Messages are not lost during broker crashes.

There are other factors to be considered such as cleaning up messages of aborted transactions, write amplifications, ordering, and etc. We will discuss the solutions of how a TB can be implemented and what are the tradeoffs.

### Acknowledgements in Transactions {#acknowledgements-in-transactions}

Many event streaming applications (e.g. Pulsar Functions) include both consumers and producers, where the applications consume messages from input Pulsar topics and produce new messages to output Pulsar topics. To achieve \`exactly once\` streaming, we need to make sure acknowledgements on the input messages happen as part of the transaction in order to achieve atomicity. Otherwise, if there is a failure between acknowledging inputs and producing the messages to output topics, data duplicates or data loss will occur depending on the ordering of the two operations: if committing produced messages happens first, then the input messages will be re-delivered upon recovery since they are not acknowledged, hence data duplicates; if acknowledging input messages happens first, the output messages failed to commit will not be re-generated because input messages has been acknowledged, hence data loss.

Hence, we need to include acknowledgments in transactions to guarantee atomicity. In order to achieve this, we have to change the behavior of acknowledgements in a transaction. Because currently all the acknowledgements in Pulsar are only best-effort operations, ack’s can be lost during network disconnections or broker crashes, which would result in data duplicates.

We also need to consider commit conflicts between individual acknowledgements and cumulative acknowledgements. We will talk about how to enhance consumer protocol and cursor management for supporting acknowledgements in transactions in further sections.

### Materialization Mechanism {#materialization-mechanism}

For messages appended to TB, the transaction implementation should also provide materialize mechanism to materialize uncommitted messages to make them visible to consumer when transactions are committed. This materialize mechanism varies based on the implementation of TB. 

The materialization mechanism should also be considering isolation levels (if we want to support higher isolation level than READ\_COMMITTED).

We will talk about how transaction materializes uncommitted messages in further sections.

## Transaction Flow {#transaction-flow}

All transaction implementations can be shaped using these key components/concepts described in the above sections. 

![][image1]  
Figure 1\. Transaction Flow

In [Figure 1\. Transaction Flow](#bookmark=id.5x2yklb5p0ig):

- The gray square boxes represent distinct brokers.  
- The gray rounded boxes represent logical components which can be running inside a broker or as a separated service (e.g. like how we run function worker as part of broker)  
- All the blue boxes represent logs. The logs can be a pulsar topic, a bookkeeper ledger, or a managed ledger.  
- Each arrow represents the request flow or message flow. These operations occur in sequence indicated by the numbers next to each arrow.  
- The sections below are numbered to match the operations showed in the diagram.

### 1\. Begin Transaction {#1.-begin-transaction}

At the beginning of a transaction, the pulsar client will find a Transaction Coordinator. The TC will allocate a transaction id (aka TxnID) for the transaction. The transaction will be logged with its transaction id and a status of OPEN (indicating the transaction is OPEN) in the transaction log (as shown in step 1a). This ensures the transaction status is persisted regardless TC crashes. After a transaction status entry is logged, TC responds the transaction id back to the pulsar client.

### 2\. Transaction Loop {#2.-transaction-loop}

In this stage, the pulsar client will enter a transaction loop, repeating the actions of consume-transform-produce the messages that comprise the transaction. This is a long phase and is potentially comprised of multiple produce and acknowledgement requests. 

#### 2.1 Add Partitions To Txn {#2.1-add-partitions-to-txn}

Before the pulsar client produces messages to a new topic partition, the client sends a request to TC to add the partition to the transaction. TC logs the partitions changes of the transaction into its transaction log for durability (as shown in 2.1a). This step ensures TC knows all the partitions that a transaction is touching, so TC can commit or abort changes on each partition at the end-partition phase.

#### 2.2 Produce Messages to Partitions {#2.2-produce-messages-to-partitions}

The pulsar client starts producing messages to partitions. This produce flow is same as the normal message produce flow. The only difference is the batch of messages produced by a transaction will be containing the transaction id. The broker receiving the batch of messages checks if the batch of messages belongs to a transaction. If it doesn’t belong to a transaction, the broker writes the batch directly into the partition’s managed ledger (it is the normal produce flow). If it belongs to a transaction, the broker will write the batch into the transaction’s transaction buffer.

The transaction buffer must meet the following requirements:

1) The messages appended to the transaction buffer should be durably persisted regardless broker crashes.  
2) The messages should be appended exactly once regardless how producer retries producing the same message on network disconnections.  
3) The messages should not be materialized to the consumers until the transaction is committed.

The transaction buffer can be implemented in many ways. It can be managed ledger itself, a separated sidecar managed ledger, or some other implementation. We will be discussing more details about the design choices of transaction buffer in further sections.

#### 2.3 Add Subscriptions To Txn {#2.3-add-subscriptions-to-txn}

The pulsar client sends a request to TC the first time a new subscription is acknowledged to as part of a transaction. The addition of the subscription to the transaction is logged by TC in step 2.3a. This step ensures TC knows all the subscription that a transaction is touching, so TC can commit or abort changes on each subscription at the EndTxn phase.

#### 2.4 Ack Messages on Subscriptions {#2.4-ack-messages-on-subscriptions}

The pulsar client start acknowledging messages on subscriptions. This transactional acknowledgement flow is same as the normal acknowledgement flow. However the ack request will be carrying a transaction id. The broker receiving the acknowledgement request checks if the acknowledge belongs to a transaction or not. If it belongs to a transaction, the broker will mark the message as in PENDING\_ACK state. PENDING\_ACK state means the message can not be acknowledged or negative-acknowledged by other consumers until the ack is committed or aborted. (See details at Section “[New Acknowledgement State](#bookmark=id.4bikq6sjiy8u)”) This allows if there are two transactions attempted to acknowledge on one message, only one will succeed and the other one will be aborted.

The pulsar client will abort the whole transaction when it tries to acknowledge but a conflict is detected. The conflict can be detected on both individual acks and cumulative acks.

### 3\. End Transaction {#3.-end-transaction}

At the end of a transaction, the application will decide to commit or abort the transaction. The transaction can also be aborted when a conflict is detected on acknowledging messages. 

#### 3.1 End Transaction Request {#3.1-end-transaction-request}

When a pulsar client is finished with a transaction, it can issue an end transaction request to TC, with a field indicating whether the transaction is to be committed or aborted. 

Upon receiving this request, the TC:

1. Writes a COMMITTING or ABORTING message to its transaction log (as shown in 3.1a).  
2. Begins the process of committing or aborting messages or acknowledgments to all the partitions involved in this transaction. It is shown in 3.2 and described in section 3.2 below.  
3. After all the partitions involved in this transaction have been successfully committed or aborted, TC writes COMMITTED or ABORTED messages to its transaction log. It is shown in 3.3 in the diagram and described in section 3.3 below.

#### 3.2 Finalizing Process {#3.2-finalizing-process}

At this stage, TC will finalize the transaction by committing or aborting messages or acknowledgments on all the partitions involved in this transaction. 

Committing produced message is materializing the messages and making them visible to the consumers (shown as 3.2a in the diagram). The commiting action can happen multiple times due to failures (such as TC retries after recovery, network disconnections and such). The TB implementation has to ensure in the committing process no duplicate will be introduced.

Aborting produced message is discarding the messages in TB. TB has to ensure cleaning up those messages and reclaim spaces if the transaction is aborted.

Committing acknowledgments moves a message from PENDING\_ACK to ACK. Aborting acknowledgments will negative acks a message, so the message will be re-deliver to other consumers.

#### 3.3 Mark Transaction as COMMITTED or ABORTED {#3.3-mark-transaction-as-committed-or-aborted}

After all produced messages and acknowledgements to all partitions are committed or aborted, the TC writes the final COMMITTED or ABORTED transaction status message to its transaction log, indicating that the transaction is complete (shown as 3.3a in the diagram). At this point, all the messages pertaining to the transaction in its transaction log can safely be removed. 

---

This diagram shows the whole transaction flows involving different components. However the implementation details of those components are not well discussed here. We will talk about them in more details in following sections and compare the design choices component by component. 

Also there are many optimizations can be made on improving the transaction flow. Those are left out of this proposal to make sure we start with a reliable and robust implementation to get things right first.

# Design Choices {#design-choices}

\`[Transaction Coordinator](#bookmark=id.s49iko5e8j8a)\` and \`[Transactional Acknowledgement](#bookmark=id.asf2ci9pcdeb)\` are pretty straightforward to implement. See details at Section “[A Full Proposal](#bookmark=id.qw636l7pty05)”. The most challenge part will be the \`Transaction Buffer\` part, as there will be many proposals with different tradeoffs there. These proposals are discussed below.

## Transaction Buffer {#transaction-buffer-1}

Recapping what we have described of a transactional flow above, a transaction buffer implementation should consider followings:

1. During the transaction loop:  
   1. The messages appended to transaction buffer should be durably persisted regardless broker crashes.  
   2. The messages should be appended exactly once regardless how producer retries producing the same message on network disconnections.  
   3. The messages should not be materialized to the consumers until the transaction is committed.  
2. A materialization mechanism to materialize the messages in transaction buffer to make them visible for consumers   
   1. How messages are materialized will impact how we dispatch messages to consumers  
3. A sweep mechanism to discard the messages from transaction buffer for reclaiming disk spaces.

### Marker Approach {#marker-approach}

One of the approaches to implement the transaction buffer is:

- Reuse the partition as the “transaction buffer”  
- Write the messages involved in transactions directly to the partition  
- Mark them as invisible and the consumers are not able to consume them until the transaction is committed  
- Mark the messages as visible when the transaction is committed  
- Cleanup (in background) the messages when the transaction is aborted.

#### Overview {#overview}

![][image2]  
Figure 2\. Marker Approach

Figure 2 [Marker Approach](#bookmark=id.kc3lb5od0iy6) demonstrates how a marker approach looks like. The gray boxes represents the messages produced by a normal client (through non-transactional flow); the color boxes represents the messages produced by transactions; different color indicates different transaction. Each message produced by a transaction will be labelled as \`\<txn\>-\<msg\>\` (e.g. \`txn2-m2\`). \`\<txn\>-commit\` or \`\<txn\>-abort\` are the markers appended when committing or aborting a given transaction.

In this approach, all the transactional messages are appended directly to the partition’s managed ledger. Additional metadata (e.g. a TxnID field) is required to add to the messages when they are produced to brokers. The broker dispatcher checks those metadata and also the transaction status to decide whether it should dispatch them. Each transaction will be using TxnID as the producer id, so broker can use the de-duplication logic to ensure messages are appended to the partition exactly-once. When transaction coordinator begins committing or aborting the transaction, it writes a \`\<txn\>-commit\` or \`\<txn\>-abort\` to the partition log to mark the transaction as \`COMMITTED\` or \`ABORTED\`. At this point, the messages of COMMITTED transactions are safe to dispatch to consumers, and the messages of ABORTED transactions can be cleaned up by a sweep process in the background.

Figure 2 demonstrates 3 transactions, \`txn1\`, \`txn2\`, and \`txn3\`. \`txn1\` and \`txn2\` are committed, while \`txn3\` is aborted. 

#### Challenges {#challenges}

There are a few challenges in this approach. 

##### Materialization {#materialization}

\`\<txn\>-commit\` is the commit marker used for marking the transaction as \`COMMITTED\` and materializing the messages to the consumers. It is also a “fencing” point for the transaction \- any messages produced to the same transaction after this marker will be rejected.

Since a transaction can be spreading over multiple messages, we need a mechanism for indexing the messages for the transaction. Hence when the materialization happens, the dispatcher knows how to fetch the messages and dispatch them correctly.

This can be done via MessageDeduplication cursor. Currently the MessageDeduplication cursor maintains the mapping between producer id and its sequence id. We can extend it to maintain a mapping between txn id and a list of message id of its messages. When TC commits a transaction:

1. Add the list of message id of the transaction as part of the commit marker.  
2. Write the commit marker to the partition.  
3. When the marker is successfully written, remove the transaction from MessageDeduplication cursor, since the transaction has already been materialized.  
4. The transaction can be added to a transaction cache for fast lookup if needed (optional).

##### Sweep {#sweep}

\`\<txn\>-abort\` is the commit marker used for marking the transaction as \`ABORTED\`. Once a transaction is marked as \`ABORTED\`, the messages of the transaction is safe to be removed. However since managed ledger is append-only, there is no way for deleting individual messages from the partition. So the messages are not easy to remove. The messages have to wait until retention kick in, or an additional “compacting” process is required to compact the segments to delete messages of aborted transactions. This requires rewriting a new segment. We can either improve current Pulsar’s compaction logic to do it, or piggyback the process as part of moving data to tiered storage.

##### Retention {#retention}

In current approach, since transactional messages (both committed and aborted messages) are interleaving with normal messages, there are care should be taken by the broker on acknowledgements. As the cursor can not be moved forward if the transaction that a message belongs to is not finalized (committed or aborted) yet. 

#### Changes {#changes}

In summary of this approach, it requires to change following components:

- Introduce new fields in Message Metadata for broker to tell if a message belongs to a transaction or not.  
- Introduce new fields in Message Metadata to tell if a message is a transaction marker or not.  
- Change MessageDeduplication to maintain a map between transaction id and its list of message ids.  
- Change Broker Dispatcher to skip dispatching messages that are not materialized  
- Change the compaction or offloader logic to discard messages that belong to aborted transactions

In this approach, we might end up touching almost every pieces at Broker.

#### Discussions {#discussions}

There are also a few more performance related discussion points:

Since the transactional messages appended and transaction commits can happen at different time, the messages of a same transaction is not stored continuously (both logically and physically at bookie side). Hence the entry caching behavior can be very randomized. For example, in Figure 2, when reading messages of txn2, it has to jump back to txn2-m1 and then read \`txn2-m1\` and \`txn2-m2\`; then broker moves to read messages of txn1, it has to jump back to read txn1-m1.

Not only that, in this proposal, we are mixing normal messages with transactional messages, it will significantly change the caching behavior for normal messages, which might results in more network I/O for brokers.

### Sidecar Approach {#sidecar-approach}

In contrast to the marker approach, the other approaches can be described as a sidecar approach. Basically,

- Use a separate \`managed ledger\` as the “transaction buffer”. Let’s use \`transaction log\` in the renaming discussion of this section.  
- Write the messages involved in transactions to the transaction log.  
- Since the messages are not written to the partition, so they are invisible to the consumers until they are committed.  
- When committing a transaction, write a \`COMMIT\` message to the partition, including the list of message ids of its messages and then write the same \`COMMIT\` message to the transaction log. We will discuss why we need two \`COMMIT\` messages in further sections.  
- When aborting a transaction, write a \`ABORT\` message to the transaction log to mark the transaction as aborted. A background process cleans up the messages of aborted transactions.

#### Overview {#overview-1}

![][image3]  
Figure 3\. Sidecar Approach

Figure 3 [Sidecar Approach](#bookmark=id.w171cbz337cr) demonstrates how a sidecar approach looks like. The gray boxes represents the messages produced by a normal client (through non-transactional flow); the color boxes represents the messages produced by transactions; different color indicates different transaction. Each message produced by a transaction will be labelled as \`\<txn\>-\<msg\>\` (e.g. \`txn2-m2\`). \`\<txn\>-commit\` or \`\<txn\>-abort\` are the markers appended when committing or aborting a given transaction.

In this approach, all the transactional messages are appended directly to the partition’s transaction log. Each transaction will be using TxnID as the producer id to produce to the partition’s transaction log, so broker can use the de-duplication logic to ensure messages are appended to the transaction log exactly-once. 

1. When transaction coordinator begins committing a transaction, it writes a \`\<txn\>-commit\` marker to the partition to indicate the transaction is marked ast \`COMMITTED\`. This operation seals the transaction. And the commit marker will be served as an index to the transaction messages when broker dispatcher sees it. The message deduplication cursor will have to be improved to involve both transaction log and partition to make sure the commit marker is written exactly once.   
2. When transaction coordinator begins aborting a transaction, it writes a \`\<txn\>-abort\` marker to the transaction log to indicate the transaction is marked as \`ABORTED\`. This operation seals the transaction. After that, the messages of this transaction in transaction log can be cleaned up in the background.

Comparing to the [Marker Approach](#bookmark=id.llk60f1xr4ae), only the \`commit\` marker is written to the partition, so the dispatcher and retention will \*almost\* not be changed. the \`commit\` marker is just a pointer to a batch of messages. The separation of transactional data and normal data will make sure :

1) Transactional use cases will not impact normal use cases  
2) It creates an isolation between transactions and normal use cases. Especially around caching perspective. The caching behavior for normal use case will remain unchanged. We can create an enhanced entry cache to optimize it for transaction messages access.

Figure 3 demonstrates 3 transactions, \`txn1\`, \`txn2\`, and \`txn3\`. \`txn1\` and \`txn2\` are committed, while \`txn3\` is aborted. 

#### Challenges {#challenges-1}

There are a few challenges in this approach. 

##### Materialization {#materialization-1}

Similar as Marker Approach, we use the commit marker \`\<txn\>-commit\` to mark a transaction as \`COMMITTED\` to materialize the messages to the consumers. 

##### Sweep {#sweep-1}

\`\<txn\>-abort\` is the commit marker used for marking the transaction as \`ABORTED\`. Once a transaction is marked as \`ABORTED\`, the messages of the transaction is safe to be removed. However since the transaction log is append-only, there is no way for deleting individual messages from the partition. An additional \`compacting\` process is required to be running at background to compact the transaction log to remove the messages of aborted transactions.

##### Retention {#retention-1}

Retention becomes much easier comparing to the marker approach. When an acknowledgement happens on a commit marker, it will load the commit marker into the memory and find the message ids of the transaction to acknowledge. Then it will mark those messages as acknowledged in transaction log.

#### Changes {#changes-1}

In summary of this approach, it requires to change following components:

- Introduce a new meta message \- the meta message has a list of pointers to other messages. The dispatcher will resolve the meta message to read the actual messages by following the pointers.   
- Change MessageDeduplication to maintain a map between transaction id and its list of message ids, and handle cursors for both transaction log and the partition.  
- Introduce a new compacting logic to compact a segment to discard the messages of abort transactions

#### Discussions {#discussions-1}

##### Huge Size Transaction {#huge-size-transaction}

If we modeled supporting unlimited size message as a transaction of a sequence of message chunks, we can introduce a setting to tell broker to use a separate ledger for storing the messages for a given transaction on a partition. In this approach, we can just let the commit marker to point to the ledger directly. The deletion of the ledger will be similarly as deleting the messages when the commit marker is deleted.  
---

The detailed implementation of the Sidecar Approach is described at Section “[Broker \- Transaction Buffer](#bookmark=id.atfdy9rs9x26)”.

# A Full Proposal {#a-full-proposal}

In the rest of this design doc we will provide a detailed description of the above transactional flow along with the proposed changes on different modules.

## Public APIs {#public-apis}

We will introduce a new public API named \`Transaction\`. Transaction is for representing a txn in Pulsar.

\`\`\`  
*interface Transaction {*

    *CompletableFuture\<Void\> commit();*  
    *CompletableFuture\<Void\> abort();*

*}*  
\`\`\`  
---

We will then add a new public API to the  PulsarClient interface, and describe how these APIs will be implemented.

\`\`\`  
/\* initialize a new transaction in pulsar client \*/  
*TransactionBuilder newTransaction();*  
\`\`\`

The *newTransaction* API is used for building a transaction.

\`\`\`

*interface TransactionBuilder {*

    */\*\**  
     *\* Configure the maximum amount of time that the transaction*  
     *\* coordinator will for a transaction to be completed by the*  
     *\* client before proactively aborting the ongoing transaction.*  
     *\**  
     *\* The config value will be sent to the transaction coordinator*  
     *\* along with the* [CommandNewTxn](#bookmark=id.gtfkqevwo97j)*.*  
     *\**  
     *\* Default is 60 seconds.*  
     *\*/*  
    *TransactionBuilder withTransactionTimeout(long timeout, TimeUnit timeoutUnit)*

    */\*\**  
     *\* Build the transaction with the configured settings.*  
     *\*/*  
    *CompletableFuture\<Transaction\> build();*

*}*  
\`\`\`

The following steps will be taken when is *TransactionBuilder\#build()* called.

1. The pulsar client will discover an available transaction coordinator. It can be any TC alive in the cluster.  
2. Send a [CommandNewTxn](#bookmark=id.gtfkqevwo97j) with configured transaction settings (such Txn TTL time and etc) to the TC. Satisfy the CompletableFuture after retrieving a [TxnID](#bookmark=id.kp4mw4ra0fyl).

The *Transaction* instance returned from *newTransaction* call will then be used in PulsarClient for producing messages and sending acknowledgements.  
---

We will add an overridden method \`*newMessage*\` to \`*Producer*\` to construct a message to be published as part of a transaction.

\`\`\`  
/\*\*  
 \* Create a message builder to build a message to produce  
 \* as part of a transaction   
 \*/  
TypedMessageBuilder\<T\>[^4] newMessage(Transaction transaction);  
\`\`\`

With an ongoing transaction, the transaction instance will be responsible for maintaining a set of partitions that it has produced to. When TypedMessageBuilder\#send() is called, the following steps will be added:

1. Check whether the transaction is ongoing. If so, check if the destination partition is in the list of produced partitions. If the partition is not in the list, then send an [CommandAddPartitionToTxn](#bookmark=id.g8xm098glzk9) to TC.  
2. The message will be written into a message batch maintained in the transaction instance. (similar as normal batching logic)  
3. The message batch will be flushed to the wire only after the partition has been added to TC ([CommandAddPartitionToTxn](#bookmark=id.g8xm098glzk9) returns SUCCESS) and when message buffer is full or batching interval has timed out.  
4. The message batch will be written to the wire with a transaction id. See [CommandSend](#bookmark=id.jl31pl47n0cf).

---

We will add an overridden method \`*acknowledge*\` to \`*Consumer*\` to acknowledge a given message as part of a transaction.

\`\`\`  
/\*\*  
 \* Acknowledge the consumption of a single message,  
 \* identified by its {@link MessageId}, as part of a transaction   
 \*/  
void acknowledge(Transaction transaction, MessageId messageId);  
\`\`\`

When Consumer\#acknowledge() is called, the following steps will be executed:

1. Check whether the transaction is ongoing. If so, check if the destination partition is in the list of acked partitions. If the partition is not in the list, then send an [CommandAddPartitionToTxn](#bookmark=id.g8xm098glzk9) to TC.  
2. The acknowledgement will be send with the transaction id. See [CommandAck](#bookmark=kix.ur9qp3cns1s7).

---

When Transaction\#commit()is called, the following steps will be executed:

1. Flush all the pending messages and acknowledges to make sure all modifications made in this transactions are persisted.  
2. Send an CommandEndTxn with COMMIT flag to TC, block until the corresponding response is received.

When Transaction\#abort() is called, the following steps will be executed:

1. Immediately fail and drop any buffered messages and acknowledges. Await any outstanding requests which haven’t been acknowledged.  
2. Send an CommandEndTxn with ABORT flag to TC, block until the corresponding response is received.

### Examples {#examples}

Below is an example on showing how to use Pulsar’s transactional API.

\`\`\`  
PulsarClient client \= ...;  
Transaction txn \= client.newTransaction().build().get();  
try {  
    Consumer c \= ...;

    Producer p1 \= ...;  
    Producer p2 \= ...;

    Message msg \= c.receive();

    // send message to p1 in txn  
    p1.newMessage(txn)  
      ...  
      .send();

    // send message to p2 in txn  
    p2.newMessage(txn)  
      ...  
      .send();

    // ack msg in the same txn  
    c.acknowledge(txn, msg.getId());

    // commit the transaction  
    txn.commit().get();  
} catch (Exception e) {  
    // abort the transaction  
    txn.abort().get();  
}

\`\`\`

## Data Structures {#data-structures}

#### TxnID {#txnid}

The TxnID (transaction id) is identifying a unique transaction in pulsar. The transaction id will be 128-bits. The highest 16 bits will be reserved for the id of transaction coordinator, and the remaining bits will be used for monotonically increasing numbers in each TC. so given a TxnID, we can quickly locate the TC.

## Wire Protocol Changes {#wire-protocol-changes}

### New Commands {#new-commands}

#### TxnAction {#txnaction}

This is a enum indicating the action for a [*CommandEndTxn*](#bookmark=kix.hoewy3ptkbwj) request.

\`\`\`  
*enum TxnAction {*  
        *COMMIT \= 0;*  
        *ABORT  \= 1;*  
*}*  
\`\`\`

#### CommandNewTxn {#commandnewtxn}

This is a request sent from pulsar client to transaction coordinator when opening a new transaction.

\`\`\`  
*message CommandNewTxn {*  
        *required uint64 request\_id       \= 1;*  
        *optional uint64 txn\_ttl\_seconds  \= 2 \[default \= 0\];*  
*}*  
\`\`\`

#### CommandNewTxnResponse {#commandnewtxnresponse}

This is a response for [*CommandNewTxn*](#bookmark=id.gtfkqevwo97j), sent from transaction coordinator to pulsar client.

\`\`\`  
*message CommandNewTxnResponse {*  
        *required uint64 request\_id       \= 1;*  
        *optional uint64 txnid\_least\_bits \= 2 \[default \= 0\];*  
        *optional uint64 txnid\_most\_bits  \= 3 \[default \= 0\];*  
*}*  
\`\`\`

#### CommandAddPartitionToTxn {#commandaddpartitiontotxn}

This is a request sent from pulsar client to transaction coordinator to add a partition to the transaction.

\`\`\`  
*message CommandAddPartitionToTxn {*  
        *required uint64 request\_id       \= 1;*  
        *optional uint64 txnid\_least\_bits \= 2 \[default \= 0\];*  
        *optional uint64 txnid\_most\_bits  \= 3 \[default \= 0\];*  
        *repeated string partitions       \= 4;*  
*}*  
\`\`\`

#### CommandAddPartitionToTxnResponse {#commandaddpartitiontotxnresponse}

This is a response for [*CommandAddPartitionToTxn*](#bookmark=id.g8xm098glzk9), sent from transaction coordinator to pulsar client.

\`\`\`  
*message CommandAddPartitionToTxnResponse {*  
        *required uint64 request\_id       \= 1;*  
        *optional uint64 txnid\_least\_bits \= 2 \[default \= 0\];*  
        *optional uint64 txnid\_most\_bits  \= 3 \[default \= 0\];*  
        *optional ServerError error       \= 4;*  
*}*  
\`\`\`

#### CommandAddSubscritpionToTxn {#commandaddsubscritpiontotxn}

This is a request sent from pulsar client to transaction coordinator to add a subscription to the transaction.

\`\`\`  
*message Subscription {*  
        *required string topic \= 1;*  
        *required string subscription \= 2;*  
*}*  
\`\`\`

\`\`\`  
*message CommandAddSubscriptionToTxn {*  
        *required uint64 request\_id           \= 1;*  
        *optional uint64 txnid\_least\_bits     \= 2 \[default \= 0\];*  
        *optional uint64 txnid\_most\_bits      \= 3 \[default \= 0\];*  
        *repeated Subscription subscription   \= 4;*  
*}*  
\`\`\`

#### CommandAddSubscriptionToTxnResponse {#commandaddsubscriptiontotxnresponse}

This is a response for [*CommandAddSubscriptionToTxn*](#bookmark=kix.pwvej5jcgbvs), sent from transaction coordinator to pulsar client.

\`\`\`  
*message CommandAddSubscriptionToTxnResponse {*  
        *required uint64 request\_id       \= 1;*  
        *optional uint64 txnid\_least\_bits \= 2 \[default \= 0\];*  
        *optional uint64 txnid\_most\_bits  \= 3 \[default \= 0\];*  
        *optional ServerError error       \= 4;*  
*}*  
\`\`\`

#### CommandEndTxn {#commandendtxn}

This is a request sent from pulsar client to transaction coordinator when ending (committing or aborting) a new transaction.

\`\`\`  
*message CommandEndTxn {*  
        *required uint64 request\_id       \= 1;*  
        *optional uint64 txnid\_least\_bits \= 2 \[default \= 0\];*  
        *optional uint64 txnid\_most\_bits  \= 3 \[default \= 0\];*  
        *optional TxnAction txn\_action    \= 4;*  
*}*  
\`\`\`

#### CommandEndTxnResponse {#commandendtxnresponse}

This is a response for [*CommandEndTxn*](#bookmark=kix.7mfmb7y7zheu), sent from transaction coordinator to pulsar client.

\`\`\`  
*message CommandEndTxnResponse {*  
        *required uint64 request\_id       \= 1;*  
        *optional uint64 txnid\_least\_bits \= 2 \[default \= 0\];*  
        *optional uint64 txnid\_most\_bits  \= 3 \[default \= 0\];*  
        *optional ServerError error       \= 4;*  
*}*  
\`\`\`

#### CommandEndTxnOnPartition {#commandendtxnonpartition}

This is a request sent from transaction coordinator to brokers when ending (committing or aborting) a new transaction on a given partition.

\`\`\`  
*message CommandEndTxnOnPartition {*  
        *required uint64 request\_id       \= 1;*  
        *optional uint64 txnid\_least\_bits \= 2 \[default \= 0\];*  
        *optional uint64 txnid\_most\_bits  \= 3 \[default \= 0\];*  
        *optional string topic   	        \= 4;*  
        *optional TxnAction txn\_action    \= 5;*  
*}*  
\`\`\`

#### CommandEndTxnOnPartitionResponse {#commandendtxnonpartitionresponse}

This is a response for [*CommandEndTxnOnPartition*](#bookmark=id.ue4e7wrfso8b), sent from brokers back to transaction coordinator.

\`\`\`  
*message CommandEndTxnOnPartitionResponse {*  
        *required uint64 request\_id       \= 1;*  
        *optional uint64 txnid\_least\_bits \= 2 \[default \= 0\];*  
        *optional uint64 txnid\_most\_bits  \= 3 \[default \= 0\];*  
        *optional ServerError error       \= 4;*  
*}*  
\`\`\`

#### CommandEndTxnOnSubscription {#commandendtxnonsubscription}

This is a request sent from transaction coordinator to brokers when ending (committing or aborting) a new transaction on a given subscription.

\`\`\`  
*message CommandEndTxnOnSubscription {*  
        *required uint64 request\_id       \= 1;*  
        *optional uint64 txnid\_least\_bits \= 2 \[default \= 0\];*  
        *optional uint64 txnid\_most\_bits  \= 3 \[default \= 0\];*  
        *optional string topic   	        \= 4;*  
        *optional string subscription     \= 5;*  
        *optional TxnAction txn\_action    \= 6;*  
*}*  
\`\`\`

#### CommandEndTxnOnSubscriptionResponse {#commandendtxnonsubscriptionresponse}

This is a response for [*CommandEndTxnOnSubscription*](#bookmark=kix.we4rd1tv1zpn), sent from brokers back to transaction coordinator.

\`\`\`  
*message CommandEndTxnOnSubscriptionResponse {*  
        *required uint64 request\_id       \= 1;*  
        *optional uint64 txnid\_least\_bits \= 2 \[default \= 0\];*  
        *optional uint64 txnid\_most\_bits  \= 3 \[default \= 0\];*  
        *optional ServerError error       \= 4;*  
*}*  
\`\`\`

### Modified Commands {#modified-commands}

#### CommandSend {#commandsend}

This is a request sent from pulsar clients to brokers to append messages.

\`\`\`  
*message CommandSend {*  
        *required uint64 producer\_id   \= 1;*  
        *required uint64 sequence\_id   \= 2;*  
        *optional int32 num\_messages      \= 3 \[default \= 1\];*  
        ***optional uint64 txnid\_least\_bits \= 4 \[default \= 0\];***  
        ***optional uint64 txnid\_most\_bits  \= 5 \[default \= 0\];***  
*}*  
\`\`\`

#### CommandAck {#commandack}

This is a request sent from pulsar clients to brokers to acknowledge messages.

\`\`\`  
*message CommandAck {*  
        *enum AckType {*  
                *Individual \= 0;*  
                *Cumulative \= 1;*  
        *}*

        *required uint64 consumer\_id       \= 1;*  
        *required AckType ack\_type         \= 2;*

        *// In case of individual acks, the client can pass a list of message ids*  
        *repeated MessageIdData message\_id \= 3;*

        *// Acks can contain a flag to indicate the consumer*  
        *// received an invalid message that got discarded*  
        *// before being passed on to the application.*  
        *enum ValidationError {*  
                *UncompressedSizeCorruption \= 0;*  
                *DecompressionError \= 1;*  
                *ChecksumMismatch \= 2;*  
                *BatchDeSerializeError \= 3;*  
                *DecryptionError \= 4;*  
        *}*

        *optional ValidationError validation\_error \= 4;*  
        *repeated KeyLongValue properties \= 5;*  
        ***optional uint64 txnid\_least\_bits \= 6 \[default \= 0\];***  
        ***optional uint64 txnid\_most\_bits  \= 7 \[default \= 0\];***  
*}*

\`\`\`

\`\`\`  
*message CommandAckResponse {*  
        *required uint64 consumer\_id   \= 1;*  
        ***optional uint64 txnid\_least\_bits \= 2 \[default \= 0\];***  
        ***optional uint64 txnid\_most\_bits  \= 3 \[default \= 0\];***  
        *optional ServerError error       \= 4;*  
*}*  
\`\`\`

## Transaction Coordinator {#transaction-coordinator-1}

The transaction coordinator is a module used for handling requests from the transactional pulsar clients to keep track of their transaction status. Each transaction coordinator is identified by an id (TCID), and maintains its own transaction metadata store. TCID is used for generating transaction id, as well as for recovery from TC failures.

The operations of the transaction metadata store can be abstracted into a \`*TransactionMetadataStore*\` and \`*TransactionMetadataStoreProvider\`* interface.

\`\`\`  
enum TxnStatus {  
    OPEN,  
    COMMITTING,  
    COMMITTED,  
    ABORTING,  
    ABORTED  
}

interface TransactionMetadataStoreProvider {

    /\*\*  
     \* Open the transaction metadata store for transaction coordinator  
     \* identified by \<tt\>transactionCoordinatorId\</tt\>.  
     \*/  
    CompletableFuture\<TransactionMetadataStore\> openStore(  
        long transactionCoordinatorId);  
}

interface TransactionMetadataStore {

    /\*\*  
     \* Create a new transaction in the transaction metadata store.  
     \*/  
    CompletableFuture\<TxnID\> newTransaction();

    /\*\*  
     \* Add the produced partitions to transaction identified by \<tt\>txnid\</tt\>.  
     \*/  
    CompletableFture\<Void\> addProducedPartitionToTxn(  
        TxnID txnid, List\<String\> partitions);

    /\*\*  
     \* Add the acked partitions to transaction identified by \<tt\>txnid\</tt\>.  
     \*/  
    CompletableFture\<Void\> addAckedPartitionToTxn(  
        TxnID txnid, List\<String\> partitions);

    /\*\*  
     \* Update the transaction from \<tt\>expectedStatus\</tt\> to \<tt\>newStatus\</tt\>.  
     \*  
     \* \<p\>If the current transaction status is not \<tt\>expectedStatus\</tt\>, the  
     \* update will be failed.  
     \*/  
    CompletableFuture\<Void\> updateTxnStatus(  
        TxnStatus newStatus, TxnStatus expectedStatus);

}  
\`\`\`

### Concepts {#concepts-1}

#### TCID {#tcid}

Transaction Coordinator ID (TCID) is used for identifying an instance of transaction coordinator. This TCID is used for locating the transaction metadata store served by this transaction coordinator.

We will discuss on how to run transaction coordinators and how to assign TCID at Section [Deployment](#bookmark=id.kqdalafxhiah).

#### TxnStatus {#txnstatus}

\`TxnStatus\` is used for indicating the status of a transaction.

- OPEN: The transaction is open. The operations of the transaction is ongoing.  
- COMMITTING: TC is committing the transaction.  
- COMMITTED: The transaction has been committed. The metadata of the transaction can be removed from the transaction metadata store at this moment.  
- ABORTING: TC is aborting the transaction.  
- ABORTED: The transaction has been aborted. The metadata of the transaction can be removed from the transaction metadata store at this moment. 

#### TransactionMetadataStoreProvider {#transactionmetadatastoreprovider}

The provider is used for locating and opening the TransactionMetadataStore for a given transaction coordinator (identified by TCID).

#### TransactionMetadataStore {#transactionmetadatastore}

The transaction metadata store is used by a transaction coordinator for storing and managing all the transactions’ metadata.

The metadata store should provide following implementations for the interfaces described above.

### Implementation {#implementation}

The transaction metadata store can be implemented by using a distributed key/value storage (like zookeeper), or using an in-memory hashmap backed by a managed ledger.

In this section, we describe an implementation of using an in-memory hashmap backed by a managed ledger.

In this implementation, the transaction coordinator maintains the following information in memory:

1. A transaction id allocator used for generating transaction id.  
2. A map from TxnID to its transaction metadata. The transaction metadata includes transaction status, the list of partitions that this transaction is modifying, and the last time when this status was updated.

All the updates to the transaction metadata map will first be persisted to the transaction log (backed by a managed ledger). So they can be used for recovery.

#### Transaction Log {#transaction-log}

The format of the transaction entry appended to the transaction log is described as below.

\`\`\`  
*enum TransactionMetadataOp {*  
    *NEW                 \= 0;*  
    *ADD\_PARTITION       \= 1;*  
    *ADD\_SUBSCRIPTION    \= 2;*  
    *UPDATE              \= 3;*  
*}*

*message TransactionMetadataEntry {*  
    *TransactionMetadataOp metadata\_op   \= 1;*  
    *optional uint64 txnid\_least\_bits    \= 2 \[default \= 0\];*  
    *optional uint64 txnid\_most\_bits     \= 3 \[default \= 0\];*  
    *optional TxnStatus expected\_status  \= 4;*  
    *optional TxnStatus new\_status       \= 5;*  
    *repeated string partitions          \= 6;*  
    *repeated Subscription subscriptions \= 7;*  
    *optional uint64 txn\_timeout\_ms      \= 8;*  
    *optional uint64 txn\_start\_time      \= 9;*  
    *optional uint64 txn\_last\_modification\_time \= 10;*  
*}*  
\`\`\`

The *TransactionMetadataOp* field indicating the operations happened in the transaction metadata store; while the *expected\_status* indicating the expected txn status for this operation and *new\_status* indicating the txn status should be updated.

Writing COMMITTING or ABORTING transaction metadata entry can be served as the synchronization point \- Once the transaction metadata entry is appended to the transaction log, the transaction is GUARANTEED to be committed or aborted. Even when the TC fails, the transaction will be rolled forward or back upon recovery.

The timestamp in the transaction metadata entry will be used for determining when the transaction has timed out. Once the difference of the current time and the *txn\_start\_time* has exceeded the *txn\_timeout\_ms* value, the transaction will be aborted. 

#### TC Startup {#tc-startup}

The TC will be assigned with a TCID during startup. The TCID assignment will be determined by the deployment method used for deploying transaction coordinators. The TC will basically call \`TransactionMetadataStoreProvider\#openStore(TCID)\` to open its transaction metadata store. The execution of \`openStore\` is described as followings:

1. Open its currently assigned managed ledger (identified by TCID).  
2. Bootstrap the transaction metadata cache: TC will scan the managed ledger from the beginning[^5] to replay the entries and materialize the entries to the metadata cache.

During replaying, there are a few actions should be taken care of:

When **committing** a transaction, the following steps should be done by TC:

1. Send [CommandEndTxnOnPartition](#bookmark=id.ue4e7wrfso8b) with *COMMIT* action to all the brokers of the transaction’s added partitions.  
2. Send [CommandEndTxnOnSubscription](#bookmark=kix.we4rd1tv1zpn) with *COMMIT* action to all the brokers of the transaction’s added subscriptions.  
3. When all the responses have been received, append a [*TransactionMetadataEntry*](#bookmark=id.iasxbbpsygfk) to its transaction log to change the transaction status to *COMMITTED*. 

When **aborting** a transaction, the following steps will be executed by TC:

1. Send [CommandEndTxnOnPartition](#bookmark=id.ue4e7wrfso8b) with *ABORT* action to all the brokers of the transaction’s added partitions.  
2. Send [CommandEndTxnOnSubscription](#bookmark=kix.we4rd1tv1zpn) with *ABORT* action to all the brokers of the transaction’s added subscriptions.  
3. When all the responses have been received, append a [*TransactionMetadataEntry*](#bookmark=id.iasxbbpsygfk) to its transaction log to change the transaction status to *ABORTED*. 

---

One important thing to keep in mind is \- when ending (committing or aborting) a transaction, if one of the partitions involved in the transaction become unavailable, then the transaction will be unable to completed. This will ONLY delay the materialization of messages from transaction buffer to the actual data partition, but it doesn’t block consumers on consuming already materialized (READ\_COMMITTED) data.

#### TC Request Processing {#tc-request-processing}

##### CommandNewTxn {#commandnewtxn-1}

When TC receiving the CommandNewTxn request, the following steps will be executed:

- An in-memory transaction id counter will be incremented, named local transaction id.  
- A 128-bits TxnID will be generated by combining TCID and local transaction id.  
- An TransactionMetadataEntry with TxnID and OPEN TxnStatus is logged into the transaction log.  
- Respond with the TxnID to the client

##### CommandAddPartitionToTxn {#commandaddpartitiontotxn-1}

When TC receiving the CommandAddPartitionToTxn request, the following steps will be executed:

- If the TxnID does not exist in transaction metadata store, it responds TxnNotFound error; otherwise proceed to next step.  
- If there is already a transaction metadata entry exists in the store, check if the status is OPEN , if yes append a transaction metadata entry with the updated partition list and the updated last\_modification\_time timestamp, respond success when the entry is successfully appended to the log; otherwise reply with TxnMetaStoreError error code.

##### CommandAddSubscriptionToTxn {#commandaddsubscriptiontotxn}

When TC receiving the CommandAddSubscriptionToTxn request, the following steps will be executed:

- If the TxnID does not exist in transaction metadata store, it responds TxnNotFound error; otherwise proceed to next step.  
- If there is already a transaction metadata entry exists in the store, check if the status is OPEN , if yes append a transaction metadata entry with the updated subscription list and the updated last\_modification\_time timestamp, respond success when the entry is successfully appended to the log; otherwise reply with TxnMetaStoreError error code.

##### CommandEndTxn {#commandendtxn-1}

When TC receiving the CommandEndTxn request, the following steps will be executed:

1. If the TxnID does not exist in transaction metadata store, it responds TxnNotFound error; otherwise proceed to next step.  
2. If there is already an entry found in the transaction metadata store, check its status  
   1. If the status is OPEN, go on to step 3\.  
   2. If the status is COMMITTING or COMMITTED, and the action from the request is COMMIT, then return success.  
   3. If the status is ABORTING or ABORTED, and the action from the request is ABORT, then return success.  
   4. Otherwise, reply with TxnMetaStoreError error code.  
3. Construct a transaction metadata entry with updating following fields:  
   1. Update last\_modification\_time  
   2. Change TxnStatus according to the action from the request  
      1. If action is COMMIT action, change TxnStatus to COMMITTING.  
      2. If action is ABORT action, change TxnStatus to ABORTING.  
4. Append the transaction metadata entry to the transaction log, and update the in-memory map upon successfully append the entry, and respond success to the client.  
5. TC commits or aborts the transaction based on the action from the request.

#### TC Transaction Expiration {#tc-transaction-expiration}

When a pulsar client crashes and never comes back, TC should have a mechanism to proactively expire the transactions initiated by this client. To achieve this, TC will periodically scan the transaction metadata store:

1. If a transaction is in OPEN state, and it has passed its provided transaction timeout duration, TC expires it by doing the following:  
   1. Update the transaction metadata entry to ABORTING state.  
   2. Executing the aborting steps to abort the transaction  
2. If a transaction is in COMMITTING state, then complete the commiting process for this transaction. (This can happen when a TC crashes at the middle of completing the committing process)  
3. If a transaction is in ABORTING state, then complete the aborting process for this transaction. (This can happen when a TC crashes at the middle of completing the aborting process)

### Deployment {#deployment}

Transaction Coordinator (TC) is designed to be a logic component, which can be deployed into multiple instances. The main question of TC is how to assign a unique TCID when deploying TCs.

If you deploy TC as a separated service using Kubernetes (or other similar schedulers), it is very easier to obtain a unique id as TCID. For example, in Kubernetes, you can retrieve an increasing pod number using StatefulSets. 

If you deploy TC as a Pulsar Function, you can obtain the instance id as the TCID.

If you are running TC as part of a broker, this can be done in a different way, described in the below section.

#### TC in Broker {#tc-in-broker}

We can model all the transaction logs for TCs as a \`TC-Transaction-Log\` topic. The number of TCs determines the number of partitions of this transaction log topic. Each TC will be obtaining one partition to startup. Since partitions area already managed and distributed to brokers by load manager. So when a partition of the TC transaction log topic is assigned to a broker, the broker knows the partition id and can use the partition id as TCID to start up. 

## Broker \- Transaction Buffer {#broker---transaction-buffer}

This proposal implements a [Sidecar Approach](#bookmark=id.x2y17hd6i2md) described above.

### Message {#message}

\`\`\`  
*enum MessageType {*

    *DATA \= 0;*  
    *TXN\_COMMIT \= 1;*  
    *TXN\_ABORT \= 2;*  
      
*}*  
\`\`\`

\`\`\`  
*message MessageMetadata {*  
    *…*  
    *optional MessageType msg\_type \= 19 \[default \= DATA\];*  
    *optional long indirect\_ledger\_id \= 20;*  
    *optional uint64 txnid\_least\_bits    \= 21 \[default \= 0\];*  
    *optional uint64 txnid\_most\_bits     \= 22 \[default \= 0\];*  
*}*  
\`\`\`

We introduce a new field in message metadata indicating the message type. 

- DATA: indicates this is a normal message, which payload is storing the actual payload of the message.  
- TXN\_COMMIT: this is a commit marker message for indicating that a transaction is committed.  
- TXN\_ABORT: this is an abort marker message for indicating that a transaction has been aborted.

#### Message ID {#message-id}

The message id of the message in a transaction is comprised by the message id of the commit marker in topic partition and the index of the message in the transaction. For example, if the transaction is committed as ledger id 5, entry id 7. The message id of 2nd message in the transaction is 5,7,2.

### Transaction Log {#transaction-log-1}

For each topic partition where a transaction is happening, broker will automatically created a sidecar partition and use it as the transaction log. The name of the transaction log partition is the name of the topic partition with a suffix \`/\_txnlog\`. For example, if the topic partition is \`my\_tenant/my\_ns/my\_topic\`, then the transaction log partition for it is \`my\_tenant/my\_ns/my\_topic/\_txnlog\`.

The transaction log partition will only be created when there are transactions. So it doesn’t introduced additional overhead if transactions are not used.

### Implementation {#implementation-1}

#### Transaction Manager[^6] {#transaction-manager}

The implementation of a transaction manager is based on \`Message Deduplication\` (let’s call it \`dedup\` cursor in the remaining part of this proposal). It comprised a \`dedup\` cursor, a \`retention\` cursor, a \`transaction\` cursor and a transaction log. 

##### Dedup Cursor {#dedup-cursor}

The Dedup cursor is actually the \`Message Deduplication\` on the transaction log. It ensures for a given transaction, all the messages are appended exactly-once to the transaction log.

##### Transaction Cursor {#transaction-cursor}

The transaction cursor maintains a map between TxnID and TransactionIndex. The cursor is used for rebuilding the transaction map during recovery, like how \`dedup\` cursor rebuilds its sequence id maps.

###### *Transaction Index* {#transaction-index}

A transaction index includes following fields:

- TxnStatus: indicating the current state of the transaction   
- List\<MessageId\>: A list of message ids pointing to the messages in the transaction log  
- CommitMarker: The message id of the commit marker in the topic partition

The \`CommitMarker\` can be used for building a reverse index from \`CommitMarker\` to \`Transaction Index\`. If we sort the reverse index by \`CommitMarker\` we can use this reverse index quickly locate the transactions can be deleted when the segments are deleted from topic partition, and delete those transactions from the transaction log.

##### Retention Cursor {#retention-cursor}

The retention cursor is used for managing the retention of messages in the transaction log. It is created along with the creation of the transaction log. 

When a \`transaction\` is aborted, it learns the list of messages of a transaction from the \`Transaction Cursor\` and mark the list of messages as deleted.

When a segment is deleted from topic partition due to retention, broker find the transactions whose commit markers has been deleted by searching the commit marker reverse index in \`Transaction Cursor\`, and mark the list of messages of those deleted transactions as deleted in transaction log.

#### Compacting Transaction Log {#compacting-transaction-log}

If users set retention to infinite, it is making more sense to compact transaction log to delete the messages of aborted messages and rewrite the messages in transaction commit order. 

The compaction is done by using another transaction log and rewriting the transactions from the old transaction log to the new transaction log in transaction commit order.

The compacting flow is described as below:

- Create a \`Compacting Cursor\` on transaction log  
- Loop over the commit marker reverse index in \`Transaction Cursor\`  
  - For each commit transaction  
    - Read its transactional messages one by one  
    - Write the transactional messages in order to a new transaction log  
    - At the completion of the rewrite, update the in-memory transaction index to point to the new locations  
    - Update \`Compacting Cursor\` to mark the transactional messages as DELETED. So they can be deleted.  
- The broker keeps doing the loop whenever there is a new transaction committed.

   
The same compacting logic can be applied when a segment is offloaded to tiered storage. We will append the transaction messages when offloading the segment.

#### Partition Loading {#partition-loading}

When a partition is loaded, it will check if there is a transaction log. If there is no transaction log, it means that the partition doesn’t have any transaction, it will recover as normal.

If there is a transaction log, it will recover transaction manager as part of the normal recovery process.

- Dedup Cursor will recover as it is  
- Transaction Cursor  
  - It will first load the transaction index snapshot from transaction cursor.  
  - Then it will start replay the transaction log to rebuild the transaction index  
  - After finishing the replay, it completes the recovery.

#### Broker Request Processing {#broker-request-processing}

##### CommandSend {#commandsend-1}

When TB receiving the CommandSend request with a transaction id, the following steps will be executed:

- It will use the txn id as the producer id to append the message into transaction log, dedup cursor will make sure the message is never duplicated.  
- After successfully appended the message to the transaction log, it will update the transaction index in \`Transaction Cursor\`.

##### CommandEndTxnOnPartition {#commandendtxnonpartition-1}

When TB receiving the CommandEndTxnOnPartition request with COMMIT action, the following steps will be executed:

- It will first write a message with message type TXN\_COMMIT with transaction id to the partition.  
- After it successfully appended to the topic, it will get the message id of this commit marker message.  
- It will then write another message with message type TXN\_COMMIT with transaction id to the transaction log. The payload of this message will include the message id of the commit marker in topic partition. It will be used for recovering  \`Transaction Cursor\` upon failures.

\> We added commit marker to the partition first so we can retrieve the message id of the commit marker and store it in \`Transaction Cursor\`

When TB receiving the CommandEndTxnOnPartition request with ABORT action, the following steps will be executed:

- It will write a message with message type TXN\_ABORT with transaction id to the transaction log.  
- Then it will mark the transaction as aborted and mark the messages of this transaction as DELETED in \`Retention Cursor\`.

##### Broker Dispatching {#broker-dispatching}

When Broker dispatcher dispatches a message \`TXN\_COMMIT\`, it will retrieve the list of messages ids of the transaction from \`Transaction Cursor\`. Then it will read individual messages from the transaction log and dispatch them.

## Broker \- Acknowledgements {#broker---acknowledgements}

As mentioned in Section [Motivation](#bookmark=id.g2xdepks9p63), Pulsar Functions need to both consume from input topics and produce results to output topics at the same time. When Pulsar Functions acknowledges input messages, the acknowledgements should be done as part of the transaction. In order to support this scenario, we have to improve \`Subscription\` component at Pulsar broker side to make it transaction-aware. More specifically, we need to:

1. Enhance acknowledgement protocol  
   1. Improve \`CommandAck\` to carry transaction id  
   2. Introduce a new wire protocol message for client to receive acknowledgement response \`CommandAckResponse\`.  
2. Introduce a new acknowledgement state \`PENDING\_ACK\` in Subscription for achieving transactional acknowledgement.

### New Acknowledgement State {#new-acknowledgement-state}

![][image4]

Figure. Acknowledgement State Machine

The diagram above shows the state machine of acknowledgement flow at broker side. The black boxes are the states currently Pulsar has, while the blue boxes are the new states introduced by supporting transactional acknowledgement. The black arrow line indicates the state change, while the blue arrow lines indicates state changes introduced after introducing the new state.

Before introducing the new \`PENDING\_ACK\` state, Pulsar technically has two states for a message in the dispatcher path. One is \`PENDING\`, while the other one is \`DELETED\`. When a message is delivered to a consumer, the message will be put into \`PENDING\`; when broker receives an acknowledgement from a consumer, the message will be marked as \`DELETED\`. If a redelivery request received from consumers, broker will re-deliver the messages in \`PENDING\`.

#### PENDING\_ACK State {#pending_ack-state}

In order to support transactional acknowledgement, we introduce a new state for messages. This state is called \`PENDING\_ACK\`. When the message is put in this state, it means:

- The message CANNOT be re-delivered to any other consumers even it is requested to re-deliver by consumers.  
- The message can ONLY come out of the PENDING\_ACK state when an explicit EndTxn request is sent to the subscription, via \`[CommandEndTxnOnSubscription](#bookmark=kix.we4rd1tv1zpn)\`.

After introduced this \`PENDING\_ACK\` state, the acknowledgement state machine will be changed to following:

1. PENDING \-\> DELETED: on receiving a normal ACK request. However a new logic will be introduced in this state transition. Before it can transition to \`DELETED\` state, it has to check if the message is in \`PENDING\_ACK\`. If the message is in \`PENDING\_ACK\`, it CANNOT be marked as \`DELETED\`, we can simply drop the acknowledgement on this message[^7]. If the message is not in \`PENDING\_ACK\`, transition the state to \`DELETED\` state as a normal ACK.  
2. PENDING \-\>  PENDING\_ACK: when receiving [an ACK request with transaction id](#bookmark=kix.ur9qp3cns1s7), the broker will first check if this message can be transitioned to \`PENDING\_ACK\`. A message can only be transitioned when it is in \`PENDING\` state. If the message is in any other state, the ACK request will be failed with \`InvalidTxnState\` error code. This will result in client aborting the transaction since conflicts are detected[^8].  
   When a message is transitioned to \`PENDING\_ACK\` state, broker will log a state change to cursor ledger. This ensures the \`PENDING\_ACK\` state is durably stored and can be recovered upon broker failures.  
3. PENDING\_ACK \-\>  PENDING: on receiving \`[CommandEndTxnOnSubscription](#bookmark=kix.we4rd1tv1zpn)\` request with \`ABORT\` action, the broker logs a state change into cursor ledger of message state transitioning from \`PENDING\_ACK\` to  \`PENDING\`. This ensures the \`PENDING\_ACK\` state can be cleared and the message can be re-dispatched in next consume request.  
4. PENDING\_ACK \-\>  DELETED: on receiving \`[CommandEndTxnOnSubscription](#bookmark=kix.we4rd1tv1zpn)\` request with \`COMMIT\` action, the broker logs a state change into cursor ledger of message state transitioning from \`PENDING\_ACK\` to  \`DELETED\`. This ensures the message can be deleted and will not be delivered again.

#### Negative Acknowledgement {#negative-acknowledgement}

On broker receiving a negative-ack request, since broker only negative ack messages in \`PENDING\`, broker doesn’t have to do anything.

### Client-Broker Request Processing {#client-broker-request-processing}

The request processing flow is described as part of state machine above.

# Future Worker {#future-worker}

## Single Partition Transaction {#single-partition-transaction}

Since all the transaction logic are managed via \`Transaction\` instance, there are many optimizations can be done at the client side. For example, we can delay the request of \`CommandNewTxn\` until the transaction is touching \>=2 partitions or \>=1 subscriptions. This allows us falling back to use idempotent producer to achieve transaction on one single partition.

[^1]:  If a global state is involved, the transaction should also consider transactional semantic on updating global state. This proposal will not involve this part as it can be extended and added in the future if we have setup a reliable transaction coordinator.

[^2]:  If a function attempts to access external resources (such as a database, or another queuing system), we are not considering any transactional semantics for such resources at this moment. It is recommended to make external calls idempotent.

[^3]:  We are using ‘partition’ extensively in the remaining of the proposal. A partition is either a non-partitioned topic or a partition in a partitioned topic. 

[^4]:  We might consider having a separate TransactionalMessageBuilder to remove \`sendAsync\` and change \`send\` to return Void. because sending a message within a transaction will not complete until the whole transaction is committed or aborted.

[^5]:  We can enable compaction on the managed ledger or take snapshots, or use the bookkeeper state library.

[^6]:  Separating Transaction Manager from Message Deduplication is for providing a cleaner separation between normal message flow and transactional message flow.  

[^7]:  It is totally fine to drop an \`ACK\`, as there is no guarantee on \`ACK\` before introducing transactional acknowledgement.

[^8]:  PENDING\_ACK approach is a pessimistic locking mechanism to lock-in a message until it is committed or aborted. This is totally fine in a streaming application, since a message is anyway delivered to a consumer and most likely consumed by the same consumer. It is very rare that a message is delivered to one consumer but acknowledged by another consumer. So during a life cycle of a message, the consumer is actually acting as a single owner for that message. We can always try the optimistic locking if this becomes a problem.

[image1]: <data:image/png;base64,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>

[image2]: <data:image/png;base64,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>

[image3]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAnAAAANwCAMAAACYuqclAAADAFBMVEX///8AAAD7+/v09PTf39/ExMRERESlpaWTk5MYGBhZWVkuLi7q6urS0tK1tbWBgYFubm7B5Pe42ux8k5+94PMSFReUr70iKSyeu8tvg44zPEFhc3xTYmpDT1aox9iIoa+x0eLH6Ky+3aSuypaNpHqjv40SFRByhWNFUDy21J2YsoRkdVfD5KlVZEojKR+Alm81PS16j2r50t7uyNTZt8K/oarkwMvNrLZWSU2hh49CODssJSh9anD1ztoXExRrWl+PeX+wlZ1SRUl2jJiBmKaFm3OmjJXl5eXIyMjb29uwsLCUlJRzc3M9PT0pKSmioqJPT0+EhIQVFRXh4eFiYmK8vLwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACaIk+rAABDEklEQVR4Xu2diZscxZnmvzzr7m6zGtaAsNt4hWzGiMGABQIBzz479npnbI/X+2/Ow3jOZzx7cAwCjZfVikM2aMzItowxizHq7uq68tiIvCs7IzOqMpVdlfn+bFpZEV9FZWW+9UV8cSURAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAdxMlnXASbuLGh+u/ysmq9FVOVqWvcrIqfZWTVemrnCzZV8FhDmo6AYC7iZZOOIkioVsAIo+XCzwc2DggSgAAAACA7cQwFUXpRC+7itLrJ7IBkENPJwhY8KA3ih5UhXrHyWwA5FgpAp0awYExXUoHgCMjJgkbJejPM6KePVVdqzo1JD4MbDGuRM/vChpQ+rQYekfadB4maiM9GKzQdb961tiBX2qY4h+qhjXwjg3Zahy0k9DDdXYUZdc7Ggy4+rwjFkooJjvQeVDBDVX+Lw8vOvzfvqdQP03xKmQvOY4+QIMIlVKSSHBqT1G47zKYYHzB6cquvtNTPEntUJ8b7vZ9A+oMRizZMzMHBhkDrxgW3uq7UFwzqVpwNFSUAVdZx6tfWe3pOy322mA68oy0Xjd8n5fFkzsKr3V1btzxJKuH1qBRyAhuhTYc0VGPjg0yyQpeB/NIDI0WPeqM+LFtzDqiMl0v6nDSyaBNiMSRzYI3+Sc7dpRg8xYZS2XlHB957u6A5u4u92hqp88rW27kfYrGpabyn4ESvx+0DQnBufF0uEWfJkM6Cl/2aKRxWCFHwx5ZAy60nS4dOAbpnbnieE6PHEMxVBab+r7Ne4cGP9dAEkqpAt7Q5zGmF6l6bTivWRYxDGIBlYURA2KRwShow/lR6oDb7spU8wBwuJq4dLz2vie4YRRteo5yj+nMOxgxo4Fn4VsbHX3k99b5QQNoLyvefmdAWhgysHpUn5sjlRdicF93zKIJlXuwOfUtnSaG5jvAkTU37E6ft+xmPXJ13jkclQFAFnq3P9jj+uG9vFq3r/T63Lv5PbpD7rn6e0pfY7Ws2TeVnkGjnp/XZ9ra8Q9574jmdRDz3hXQSiQW0XioiutYLmvqKzzWVF2NNRBtXqkOVa1/SC5rwGn2zCVHH9jUn9g0ZxGDxlwZSyKj57I4YTgzLXJto2OovSjuAKByWADroQT/goYi470k2nClA0s9nMyEAa1m40goRUKUEqXk4/odvbpro/et0cgopY5w0VAt1+0vNP1OOge0DglRVrHyXlMceLfGI6OUOjwc8eFUADgSQQMA1QHBAQAAAJUgEaXKoUf9bRnw0CQe8wctRjZK1UJlinSjq64jFJyp+Ov0h8cFnSPq4DCdBBqFxEiDh847bzlKvzdLZ3KshSn2cAvVyxraSv6kUFWfGgWSBNuNrIfje4twLfS0OzSY81UMa6BMu2bRPBFtzbJB4wjWPvNZ5v5ahRTeBDkBHX8GgOGv3M9i15usOdxJp4PtQba6LCCcLRIIjounl9V9JyE4Md5iV7DdFN1kTpZyCpjt0NR3RHrk6YaJfUSSjPgM9AzU0Dx8H59InGRoJH4uambZYBvJ1kM+d7z/95Veb+LXroriuFbXi199kfe5N2ReravMXG/rkQB/TU1H6WmWzk10xbEGiqJp+oS6XHreG/l+EI5jBqv2e/09w1SW1vODRpOuUnkvh8K3a+jwhVp9vkjL83TBIgfvMBAceQsDR1GV6mlox5fdHhdcn3e4KDs8x6tSvTdq3r43qld2x/9ULILYBu5SleqHtkPqz4gm7H9kd3M6zw776bakRTvcGY5ZzGsdexNJUv0sQy8odiZ80XUI9ttsCGsJjutl7s19cnhokitsx02LZULelPNFOPH8RG25IM+rnVQi2H7WEhzvKrOINeAUJj1FzR8+MDzzJeIO4h1WO5/YvjWYx+eueXZgk5G4pScGB7yKziAj2CWknz/PM8NLhR5R7R+qZrBaOkGgX+G4BdhQTiglAwnBpRmO+duY47I8nKNebq+FSidWB4aCMybd6cw68e4Obxh6ewpDc41jdcGpLnWHNo3jxpd9HIcFJ9yVPkkPn/Vo4e/cykhUp3FDcOz3WWvUza2rwTZywr0UodvHvSnzQIvB8axjHGm9I9ZGczoLf1cR6i46NvVioWiq3TuIXnnMiQ72jqzh1DLnpCma54d7k9kwHGZlRZvWYu8OVbLwJi+iASshUWMWIjv8pZumZfFdQdx+98j74A459kJxezNyTT4xqeNq7NjqTV1XVRa6xaRJ/f7UMeYudfQFdTV9OKOuqlgj1Zq5ymI4s/oLZqxZTK69OS0UwyRL12xy9LljHpM54wLWdaZnm3atNb8uBLdRyN6OcHN8sqMJcarG3Jf3Uh3MtcViNPcCBCbEBbO27M68b2mGzf1WcpyUvUMdzlXH5I7PUDsHzHjhsA9wXH8rL16IoWq2Eb7Ttvy/iUJWQKG9dBJYh88r8XASgpNZbZhFZ74Rj0eC4CqiWHAySlk9aACgBHdVcF7vBgAJ7p7gBnOpChu0i5W7RaSZ8TBjzYY+aCx3T3DQ2vooxY3vbeXuValgXTRVtnd0C5EQnCthA6rD0D77NJ22HVQ1eI8h9IrpnJgCmKD3//QH0mkNQkZwoFqM32W1nLWRH9NP9o6b3J2U9dXBXaX/Ed2ms+M/sMMvHeis9twzP6H7Zr//Pd0/a35l0uDmaYBycg77qaJ1hof3fUGbdveOHjwk5wujjmv3J92evnd0+Ef+bFVlslmn7HNiZvZawMPVjX2oksZXHS3unWl2d2F7Le3uEU3u++2JyfjNA224U8PufGIYH/vTAT1m9NtEdkOREFzuoiywPof0sfvFjCUfW4uMUiQEB+4WD9CWdriVAII7RX5zlj7O3IiqwUBwp8fuFyf3+nMcWgQEdxrM+R9tMrU79MmJdW7NBoKrH40+4f2ffBHk4RfbVqlCcPXjnKHPf28MftMl4tvnTWhHp6lKaofvHkVqf6jTcKg2VIcSgSypVSwPPTU2cBGNogyOdJvvaTEas5eudxMc77c/OAxdwMbt5168iEYGGcFtNxsouO2kGsGhSgW1AsGBWoHgQK1AcKBWIDhQKxKCk5kDAICcUiQEB0B1QHCgViA4UCsQHKgVCA7UikRcIbOv4QYj8Q2BHEU62HKlgE1CpstDBlSpoFYgOFArEBwAAABQF1XFJ6DpyCgFbThQKxAcqBUIDtQKBAdqBYIDUsgEBDJAcKBWWrZZFFiXihwcPByoFwgO1AoEB2oFggMAAAAKkakuJbpFquryA41HQikyogSgMiA4UCsQHKgVCA7UCgQHpKgqdITgQK1IdIsAINXjIQU8HKgVCA7UCgQHagWCA7VSVVtwm2nBNdicjQL1dEIr2U8nNItb6YS7hGanU04iUaVW1eUHmo4joRQJwQFQHRAcqBUIDtQKBAdqBVHqKaMoirM5nRZiqnoGAwR3utzif84p81Ryc5EQXDXKBtmcV6Y964NHlHE6YwuRUQracHedYTedkmQxpYlLNyR6sJoBBHfXeTevvjzvsD8dprt0RlOB4E6Jrn/lZ/zP+NxSVqOB4O4yTFiKqg5IucX8GOk9nnbrVsd0PrxlhDbKTanmz6niVnSGENxdZuqQ6zhjpqj3NVKUiZ86m8//A90MbUZ0LhJf04Hg6uI8qd2dfvzaio4GLvWaEKRKLZCREBxmi1TC/NxN+/ggncrR33lMYl7PFoDZIpuE69BNJbMhdP0xqxEOTgoIrjZYM83NcgH9NukNgquNQeexczfNdCrT24026Q2CqwHfrb03vrM4/37KxQ12b7n7W6G3qlryEmOpoCSq4w7G3QsHPHBQvZ7eCOX6OYf3z7Gj6VJGY4Hg7j43if5kZ8J0RyP3nfMLk9UqOwcqu/K9ySLqjHto6S3NpSJHudUod3fVVs9YzNwR0ZFDO+zlYjZkfw9U728v6vA9zAxgq+FWBQMZVc2Hg+DuuuBOn00SHIIGUCsQHKgVBA1AimoqVHg4UDMSgquqyw80HRmlSAgOgOqA4ECtQHCgViA4UCsQHJBCJiCQAYJrBYm1FKeMjG5ldtLcZmSuwZbTO06nrEqdY6kN1xsAoFK0ChpgFRThIePhwJazSVUUBAdqRWaxNABtiKxAS6mquQiajoxS0IYDtQLBgVqB4ECtQHCgViC4NiDTmi+gqv4zCA5IIbPZoAwSgsvc1AyAE8hsPC0hOOKPEgCgEmQEB0BlQHCgViA4UCtVRbug4SByBE2lgl5D0ApklII2HKgVCA7UCgQHagWCA7UCwbUBmdZ8AVX1n0FwQArMFgEbB2aLgI1DRnAAVAYEB2oFggO1gifRyLHtgVPx+Us0+KsAHg5IUVUEKuHhqtpsc7t5PJ3QLK6lE9bBVYqVIiNKACoDggO1AsGBWoHgQK1AcEAKzBZpKIqqqapE3wEZ9d66OmeLFMe6oDI6mu2oxjUjnb6Mcu3avxSYnAIySpEQHKiYfl715C6ILJXy1WQMnk4nbQsQXO2or+fVmN5DPMYFfe32dJZO2hYguNoZpBN8gjthLaeGDFQvX/e16gistoC8Hxu4CyjdV+lNesJ4kx0/rv+U/X2K/XnBep0uzaNHFLEqdRK9w7x6ad5/ldm55hWiby2ijK0EgqsbVhk+O+2Nn1NeuzSxnuouZtbj12hhv+C8Rt8MK1KdDuM3zOnKxdnzs6s/vWQ/b7+ub7ngUKXWjOuwJpgzpvExXaGBMfcbY878gIlthx8qO+a1q08st+Hm88P5CzSZHx5/68opuYiqIlAJmwrWmIGTOM9duvbPi3n0enyRvGrUPXC+9cJbnvaWOPL+Lirrgb0LyCy3OqXfC2AKU+n5RM0ZYzFZvfLcuMObeXQxlqTahNVMEh4O3CV69Kr48jfVEzT1e20Di0tXOuo4nerxzBsTmj/Bj5rg1ZKIf2LgLtO5OqMr6RZZ8PoN5glch7O9PW7ZQHCngKcq9c0XnRfoldQNCHp2eXSwWVQlFIlyZAJiIM+L9Pq1vU73rWcPaXaR3jINFpEqHaVjkrlLi59e65nXrr3wXCw4g2hHo86ABh3Sdkhlr1VmTKrZl4gKq0ImApVRikQxgKFUuIhGNSyHCYYWToe/7E753zlPoXlfn7q6zXuHI7iRZXumM407wJknOs5c4g7Lca1g9LaypVQIGurHmXnjDRT8DcTl/8NjiLgjJJHh/7X90S9ewpYiUaUCUB0QHKgVCA7UCgQHakUiaKgqPgHbjIwGZJQCDwdqBYIDtQLBgVqB4ECtQHCgViA4IEVVCw0qKqbxNP86FfRoyHR5yAAPBwComKL6sCCbU1SELPBwoFYgOFArEBwAYDtJL0HLQsKmquYiaDwSSkGVCmoFggO1AsGBWoHgQK1AcGDjkIhkwUZT1NFQkM0pKkIWGQ8X7XUMQFlkBAdAZUBwoFYguBZQUfOrEiA4UCsQXCsomB5ekF0zVQXEoOnIKAUeDtQKBAdqBYIDtQLBgVqB4ECtQHAtoHD2hUR0WSeF5ws2m8LuiqJ8kihDEhkPh9kioDJkBAdAZUBwoFYgOFArEByoFQgObBpVBcSg6cgoBR4O1AoEB2oFggO1AsGBWoHgQK1AcC1g227ytp0vSFHYXVGUTxJlSCIjJiedAMC6yAgOgMqA4ECtQHCgViA4UCsQHNg0qgqIQdORUQo8HKgVCA7Uip5OyEIhxe/8DVym/Ktg4zH/VZ5hNa/8j8vMquYVvk/hKwAAAAAAUAmF3RVF+SRRhiSIUkGtQHCgViA4UCsQHKgVCA7UCgQHAAAAAAAAAAAAAAAAFVA406MonyTKkAQdv6BWIDhQKxAcqBUIDtQKBAdqBYIDAAAAAAAAAAAAAACACtDSCWkkZoIUliFJVeWATaZIUEX5HBkbCTDSAGqlIt2CTaXD/puTyQ9nqSyO7lVxc57vLFJ5IZ6NV4Z5mM4DYBlTCfFEl2YYZSvddF7IKDLZSWetDqrUhhPLKFNQR/3oULi/+GH0znEyeT0guIbj7zC+fJTEDg/6R8nkJXi1zDEi4/WB4BpO5MIEgrLCgxwxhY4t00euCATXdMKwUBAeWkG7rD9dTk8SRhMHS6nrAcE1ncDFCRxcVNPmOLhKgeAajy8loaCCkEAYMlQMBNd4fCmJBeV1lwgdoMeu99dIpa4FBNd4jrgL64oF5dWpQgfo4Ve7VcQMEFwL4C4ss9fXxxs9EDtAjh+mVtALB1qBovTSSUmMnFGGgJ1qhhkIHq4l5Dg4rzrNd3C+c4ODA5IoyiidtMRergP0MLOHYlcHHq4FdLv5szzGxWLqVhMyQHCtoBMOhgqws4dZkzioUYE0RgU9aHvphDURjLA1hqZ/v9PGTScUURSebD0Sv8zPaT+dtMwtejydlOZasUVxIRJG1/bTKSe4VfR1OLcKPoefitSlWxW04UCtQHCgViA4UCsQHKgVCA7UCgQHagWCA7UCwYFageBArUBwoFYgOFArEByoFQgO1AoEB2oFggO1AsGBWoHgQK1AcK2homVX/j4ja9P4Keatx+y8Exw9ak2Wclai63ziH3x+ryvafVoGCK7h9H4WHb5D3xBvaVOA8XF0+Ak9UGLJIATXcAw6p/v75StuvL/qyth0JtiAhBVTph0GwTWcBZmBP1IdytlWtQCNXMuvSUf5q/iLKCNWsAXMqlky3y3YQU4aCK7huH8yCI6K93PI4eg/hk9lK+fgUKU2nZFTiYdT/m8pvcbAwzWdt2/5vkm7tZy+Ip997m9IuLP6YvslILimc+HCL1ilqnR+kc5YjXvu+RUvZvirdMaKoEptOEfkPKR3F+r7dD7raYKyDOjwwcmue3ybzq7dl+cBwTUch///50TnnGK9DcQRLQsVDpVfEp3pl3wcDarUFnDM9KZJdGtM8sesRkxvg5J6g4drOjt0aL5/rjfp5PT66oHbUd94Whf4OJXcwa/OODTfvZPOWg14uKbz9r85+4uDxR3nVjonpPPTSAXuQrQ5/md/mH7JcqzJrGSUCg/XeM7PvX/m/0E0ktoNnZ9Kc/KNMzg79mrT6b8vbgvmAcE1nSg4FelNbuwgCk7L6Q1VatNZ5AcCnOgpDe7TyeQltKo2S4bgGs6s+BkMVqgzVzx81a1KKKhSG87QiaQilJMZVpP/ItxqetyLHmYjVQMLgeCazrvR0X6cuMyr3/R2v1fyHufwm+hIYnPzHFomuN3g59n96Mui/qRuUAdZN/aX0hPowXD460+JGuKBhZyJ0K0UFxOf7NdE3WzfOA4ORqJvbD3Zef3pmWJcvZzOSfBA6CjLLIygtglO/2V0+MvsMcHB/OfRcWeRWQkNlNfCw59etDJNhpEFM8nsaEgUQmZ2Iczon6PD7E8y4pP9uWioVA3fJ1Ik2XT84svP6K9dFHT6ekzCYoZLyStTVVtwO/g0cXw7cRwzvRkfv59dxeixVOhq9g82YUFXE8cxEoUse4NMo8TJ0vuJ4wQH0VhU3kNjDp5547XLc/epdHqEE8k929VKk/U1GswDx/5lH/1a0BIZ0DfGvon64X62z5jS837FrL71uKif9PmjoJC3BPXlNGkiKISmyWIyjRInu5wRocaxQlfg4zQa2q/R5dcEp+oximOFcoNb7fJwFNwfMUcU3GMxC4k47VCikCITZlRoU3iyNAxdiv5BpmQZev/l1+jpoydHg5+msyLGYXde5+OMqn0FWubhQtTsFlyy3jFFjaLIRM1uniURmsSfIzRZ5ZOEJ0v0QXQkVMqrdHExIzu3sozXpRb/3PJol+C+GFYqd0Tfe/RweGun2S04dnNfCJpF1lAkg9BCbBIVIjZJ9I+JbIpPlujRYIzgWKynSxOvnNzw897g7YZQ2XK0q0r9ONxfQ02GD0t8EPbM7yRb5Eu8Esyo6CRjgyVCi3ImfG6Rj8hG4mTJOvAR642iwYgXE4kpXMenpN5aJrh7Px7yni1l+Fk6J+LRDzp8XZ3SeTudE3HpFW/lnfJmOiPi0iumV4hZyoSpssBG4mQPcv2WxzyKAsSzK53iIVk52iU468tzZcTCsuwuEQ/layarXzRd0MvAcZ5S2GXbDdd7ZuA8pXGTQXaXiAc34YXkmBAVFcNPlhnknWyvM8w5UQ8z2hCpyLIC2iU4547d/fXO8NMzX0rnhBwcTB1V6/zi5rmvprNCZjNr/tau+fJrL6ZzImaWyk1eu/xiOidkxk14IWITiWL4yX6odXJOloz3343aq51bt27FOTG2GSCouRmjaCiVHXfi45URNZ6bin1036/4XMIviWuPMd3isxbdr4pXAcyfeZno4p0XxT1SY2ImF8durgnzXPkmxcWM6bHrBSd77vp+cDR7qP+uktGRItZZRHIQQv3dFzIKkaRdghsdKgNWnWqusLWikqPwNjS7L6JbyEzMN4j3MohUQKqjGMzEcYUm/HN4ZJljIlEMMzCv554sQ/26Eg6nOha3PcHlINg1ndDwJP24H+nOvUbeIFg+7RLcr5nYmG9zP7//I8FIw5A1v8/tEy1uPSIavDfepMvc4wwWwmH3t8gzGbz8rMiEFUKXWV6OSfRJYpsP2cky35ZzspxJ/9YjgZKm8dBqzHweiDBcTJPFcrNX1I1ZTM5HNJKzNvNtBw9md2v5nOcdCPZDeWHZmHucO3nX7iI3ufNEvgkVmRQXc96yi06WecDHbgQhTi/PFZL9mmzUcFvc71dAuzxcuIz3kHeOZBMuGHaiidcnuKz5NVxG5RQQWjilTIiCppvQRuJkOePBY9cf3Tl0BzPr0YxxAjNcp6Dm9P2GHb8B3XyJi2mX4PSw6WYLdw1VQhcwFu5PqgYtqnEwiH+S0KKcCUVNN5GNxMl6cMUxeb5H9HA6i6P6Yad65bIruizEO4sSZJ+PBO0SXPyzFF3YuRFdWNEttKNaSXjV43pLZBIXIjSRKKb4ZAPGgwuTmzeJwrbcMlfCg4V4HOFk02892iU4PbqFQ8EdMuM70hFcfS26y6lffUycLjKJCxGaMKOoGhPYxGcoOtmQMQ0uzMjN1FvIszl6E0/fXJF2Ce530dHngiiV/jU+3I8Pl/iX6Cg7dkxalDEhuhYdZdtInOxBOKtP5NOZI33Oz8tTY1UOrnVRasi96YSTPJpOOMGldMJJqjEptsk9WWVHtIGDh3WsyvkdtQK1yH1SU4imJw3V7DoqMeOnI/zBhzOLTFMU1EVzj0qZxH5NZCNxshzlbaED5Khv0cXweGeaPbXY47OSK7Y4FWh2i4iacK6oSbKIOkyMfjI9gRVWTh3hrYmqL6FJVIjYJLF8QGRTfLIyKPRk1K027miyfXHr0S7BRVHqJHiOzwkmkStR30mmJ4gCzHkU3aWJhC00iaNUoYlEMcUnK4NOtid/PlPTPpxnO9OqaJfgom/b57vrZRG3doQRWyecLCF+JEs0nUJoEhWS031cXEziZLMNpHAv+//68zC7Ve0ikk272nA9/7bMldvCjqtgZwRVFc8xM31HqV0ViiW06L0iYyLGPxdxMYmTzTaQIuhSCYb1xyWkK0G7BBfuV3Dm/onoBoU7I5x7RGjyavDvs4UWl0uZeJMAOEIbiZPlmOfF/poxv+oFJ+abT/NR/K6gvcg4K/yZytMuwYX7FRyKJ/iHOyPkbHP1fNDgF0eGoUXO/ZEwIXrC/0doI3GyHFGEFLDwppDoxqUr9Cy5V7K7/DjC81iBdglODwcnhSxytqwKsOxCk2hfBCEShTCjIptCAw9l5My1qdgFOhevXlZmV+lJotfTeQmUkW3blrgYOdoluEK95UyXiMib4BMgdqAhEoVIGBW4Lo/B4gPv38finpg0C2/O78X5k/1X6ZlO9kXSjI//wP+9b154Vrnc3Rbi9rFzy4ge1d3byeyh181wzyJ2GzpxuJnA7F5LmiSzAvSEiaAQZlRUTOJk+dkmcmICvdF1sW9xX3yGnn1iTvbhN785zdZb9ITe30ZntB7is2giRqKtIxruvnku3IPD1t/J6qFXroZtK+Y+Xskc5rwaNdE8kwwLJWEiKIQZvVFUTHyyZP8s62Sp+3M6pzEbvRctbTjJHXrcbw0KK8zdX/qTCTvub8uNNrTLwyWdhGgv0vOaHfS1zw+/knV9DHrOazrx9x88/s1Urs9zh9zE5CMBB9+kjOmxRsKEF5JhwoySxTyeZcNONlyuOM88WZb29QXXpHX4tdgbnkS9Fk8UyGJBf2TxwbjZ/MvRosK1yDrH5mKPEnySXQXRVNPD0R03azc0y99HLVgtmukTgp3WrnoDTpkWUSFik2j8y7fJZqpdD88282RZOzAsfCru7yhGpfAk7kgFKkLaVaX+dunVseBxQNOfhxXVIKsngK9NpvzwIhzjDIKHjI+JChGb+GNNnDyb6devn/OPMk+W4sii5AhC2LYrWUy7PNwyn4qGqR++afs/RDfeUj7BFS9z5F+7TDfpW7B/uUmmRVRIjknkDjwbEZOHb+adbNSKGOwI2hBSRH2OWsn9zNvl4e5b/rqCids0f+QGPaocdbR3H8uI2SwymcsZzJ5/9SK7x69czvIr3IIGz7uvMZNXslYKRIVwk16mCTPqJIrpZdt4Z8tOljo3sk6W5uf8Hr/+e5kxhSwjteddrf5HJacotUtwmkhiKY6//jN/+kVWs8e+9PqzC9V+41v+bqpZnfyXXr/sMJO56plkzdy2uYnNChGbMKOlYp7OtCHufcQnS9OuXyE7D5WatXtn15+/Zzwo/JnK0S7BFXfIBky+MuDjlF/PbKjNvA75S5Nn+T/PaBk6mPm7Jzw+4CaZFoHJpUmOCQWbMPjFkC6wIco72bAJJ9NHnEfgPbOc6Eq0S3DFVz1cAuAe7ZMoMHAeVxSH5R3zjrHMIp3HiZvQmPedZVp4Ji4rJMeE987FxWQtSpA4WVLMYN5LND34JAaZGZ18S3h7ZHCiWdPr0S7BVYUr6MdIIGOSTshAopgCdq7TY1NybtIH++msEJPV2M+KpyJ4dG/z2SLTT+njcm24ciHHNjL6vPCJn7dupVPSFPWTkpRJsYVEMWr+2faun/vandlssX8hnROh8hbi6wb1cj7K+O2ZL+8dHVl796RzVqR9gmsZBul+HXhwXjTSYNATT1xif6fPijvZbFL85ptzttxIA6rUhjOLOkvm4g1VWIv0WYfcSeZmXh79TthEHJfpzoOHazzacdi93c3tFrHfyG1UOtFyNh2CAznoNwMZDYRzJ3W+iIarUewCWbQQDFloblZHtzyoUhvOwcM3wsP9RHISy98k/VpO3zI5X/woPCwXpbZPcMfFfeXFnfK9J6owKbaQKKZXcLbzCxPvIQ6PToTd3sGWq3r2eIfP9B7/2Rb3KjlGErRLcHx6iO3JbSQQndobhzu3ZGxO6sFN/E5YkQUNxmE/rchE4nP8nNxiij+HcTC4wKxcwRfmBPteiDuGOY7y4JiGecVI0a42XDyF8deJ1CTDeGj0w0RyEiNq6gSr+E4SN4ZEJnEhQpPk6Qpsik+WMz44OMhbs5NapaAK+kbsQ8c5KKu3lnm4+AllQoIVAHnkLV32KbaQMUlu15WNxMnuLARDXjGpWSh6VnShalmTFNagXYJrI3o02U70qAAaLTXLvKlVJwhmjFLZreLaJbh7wos1tERD0BfC29JxRW2aMJYzu6JbGEV7pUwS23UJbCRONpi6xNmPE5foWEElqtr8YJHh4Iji3X8QpcoTX0rRzzTRRhHtkRUnC2uZYpNE2SITiWLisxWdbHIPOVF42QlXO3RU0c8wubVeuX64dgUN0SoT5xPBiKAStZi12DUsE5no4UzyE0SFCE3iz9EFW3GRRDGJkxUNlMYb3gmJVOa8IYgX/Pi+EtoluLB1rM1FLm4YanIgcCosdgyDx8Tj5FOEFgOhSVTIIOcOFBaTOFmRh4s3vBPvNBhqsi8evI93L3GKFZxH9i+nqajBWrfb4o1ZJsEQznvLyUmMoGZ+TTj4GFr8s3h8MmEiJhi2FBaTONlsA74+J3o2hegnpAT71wxmxHsHM4klr/ZLdY20S3Dxc6DvF8039HrlPbKfaUCJh+8JJy1GFqVM+Ar+AIGNxMnSzHehqnnjoVROiBU07+w3Lon0xryf7/zm2u0HUzmr0S7BnfFbOsf0mbBz6px3f5jneFto4o8EKfRmgYWECbuLYhMK9noWF+OfLDtb8clSMMGcxIGS/cwbl+cdsq4I62VKPtytlINrmeD8pW6Ms7cFwf2RHxCyX/z59/dTeQGLRXDnLvob+WVwHN5boUlUiNiEGRXZBLXkTHyy3Pd1/RaraDSPRQ1PKuTa+lP8QXEi7j/edTytCZ5RIku7BBc13CxR2BU3hUSNojij641lZlFsEpcuCi9JopjIwBUYEL/DQRSaIxT7MC/XYxou2CopuJwYqYFEQZgTbZWRIn7UdsbuWD5mOANxIVooFTb2c0yiQkRdbJzCYhInm23An6keHmXshZNANIIa4ET1sfCHKEe7PFzQ92kqvz0jamQr/kjQtHf9nEgKiq/FjmC9PMUWc/Hz+RImooA5FL24mMTJmpkGySiVop29TtDn617pyWyn75HomNkttTa1XYKLN7NJPMRtmevRUXIzuSXeCA+ET+yOLKRMEqkp/JmRJCwmcbICvUWLvwfOu19bzohRPb3R/35GE5ZCXf8MNPU3X07lrEa7BBfywFw0zBPxDW9cMZcnCy2eP67CpNgm72SjCb/C5dYD5ZKtdKedxZVn01kx0YTfkovv2yW4fxdUGuIf8tEfB3niem4RPHwveH5LFqHFofCTokLEJswoiE2FNsUny6LtwFGLl1RrrzzOI90ZvfDKE0J3ezaQ9JGwGDnaJbicRkqAI7i1CSRMJnrmFJ8EEoV4la2m6YrYdCJulkXkTb30mV4MAs9D8dCWYLud1WmX4OrirUQDXNOznsqhvsW3DfePmUWWiYf+JvE1VQI+TOxg0zMyJ7wdFnukLvdwnJE4BHVFJ7gq7RJccuZ/dkyQCOqo52Re5U5CHVriyc5L9Kahjxu+nLUbtEKXnfCtg8WVLJOA5xc7WUIKsCIfN8/eVHpUvNNq6NaUV8RjqUqn2JdK0a5+uGQnq5PdL5WoC9X3s7aUXFq+2b2aPXfiGasb/pTvvJh1kZnbOw4/6mCSaRIwn74SlXWCx/p2+J3sr2YX4nZC0jkhB5e7o46hdbrh3sRZWMOQdM5qCL9KI0neEVvLHGxIpI2/oWT23yc0OX7eUrNMHMPuhj7OIu1kM8oMOnyveTN/D3IaT0RPKD99URAaTvXuNPRx3eylW/EA/36cuIQ7C56Hflk8bzgx70EwKChJuwQ3Tyruk8yNgMyEI3DeydxySEv6iitPJ17EjHXNCJQ5zvQ8fgNO8VtNrrjxxHDU51++KOhoG3f5LjT+IfEHKZwgGOBnxWS2ITiLi960lKeze5d9gnkPTONF8VA+7RLc8mN5P4uWOCRYnuf7drRqIMHyFN03s5tf1pH2v32/ZGZFBPOrXg+ESVd5eCEYZwsZ0+MLuhZ2pCwzJf3nX/XcciczaAhmI1DOWJ1Kc+87zGaZLtJjEE0SKTnFvF2CO7t8sbIu7/lldWTdwzC6LMB+8uXL6mxhvPlMRnvboYEzZv896165ZDqvXsqsmDt8pCEabbAFWxtNvv6zc0r3aPh25qbSidkIgveT0Qm/5VvZPx5K9vRZomLkaJfg6sW+7M+yzGoqLp55lZ5RnStPDTyHKfPEtDeeENzpyTm/nZalWRpFv5lO1npTD6d4Q6Sg7mc4Q+E0JxnaJbjsEcklhGubYvIa+Eu4L7zC/j6Z2Xaae8OplybWC4srl5Xse7h4mtyrF6OPEzpW8+EPSLiptOsP8FvaO1/L8LQe8exjMUFXdtf6BGOp8mSEiykyxbGMRKP5ov9Y5TF9U8l2cMxRPNFVbdPibulbolaRM2MNr7zNY877bmdMX1GEm0rHjVKR3uiSf1k0N7vj0SNu/mZV3PK0S3DFchLctSTZAlpiHjoj8QCmNwHE/7S8s7Iu5uk70kfO57Ao9X0vUn0/nRMR6Ez5P0+kMhLwKHXKI9Xb6ZzVaJfgtg9bQt65LPQJ7XuSToVDMWG6Tm9lR8LEO7u5G+b/pQKvVYHgGk5cj2aPiSTQc5qJsZ8tbJXkA8E1nDhKNcS9bD6Tp1xh7R5Hqa54MY4MEFzTCdaldpTrX0nlxASDEQNbPNYQrEudGh99IZWzGhBc04ljBWFgMXo1PLrcmQus4lhBYCAJBNcWHp2JGmi7L0eHrxUOo9ybs8OSDBBc0wk3gRC3vKb0LT8WHr0s3sE63BujQI+FQHANR6ITww57Ag+Z3ASCE+68tCoQXMMZZs4/WMIKd+N0hGP32eO06wDBNZ1gLJX4zJeBmzmcLOoRThIvC3L4hMCsYqSA4JpOPJa6T9rboiH+QpJ7/Lof0R8Je+wKgODahP3YYm3XlEC5T1m7GAiu6SSfPF6iJZZ88ri9ppfkQHANJw4ZSs3UdSK9lSqmbcsEW430vNF8sEwQ5KJ3Z8FKwUcmJZxTR5/5KwU/v79MMRBc01H/NTq8kbP1dBGj+Gl4H4l35JYAgms4rAZ8xL/Jlp47tzgfFm7c7zfjutMSxUBwjWdMDwX+qGg6XC7Dzx4MBmM14fCXFAgaGo5bSh4Rbs7g/0pAcA3H2Y+OEqkrcxjtKFJSeBAcqBUIDtQKBAdqBYIDtQLBgVqB4ECtQHCgViA4UCsQHKgVCA7UCgQHagWCA7UCwYFageBArUBwoFYgOFArEByolYoWK24sTf9+p02ZFYOgwShV/PKqKANVKqgVCA7UCgQHAAAAAAAAAAAAAAAA1YChLdBOIDhQKxAcqBUIDtQKBAdqBYIDAAAAAAAAAAAAAACAasBsEdBOIDhQKxAcqBUIDtQKBAdqBYIDADSXKrpWGge/KMHWjsH1WXqVkxW8Er/Ie5WTtaZh1gnL2km8OpkVHOaAKjWX5cujLb1aQvY6She4jLTheuXL2skbglUIfrlgRWSGM2R/mQBUAgQHwGmDXyEA5ZBpXhVSRRn4MYNa0dMJIEDjv2fHCV96F8qKcsGaQHAiVGVBZM7ClzZRV4HgwF1EUZReeKwqSj+Zt22gDbcdTI3gIPwXlASCyyD8JRvRobredTLWe9u2IuNG23VFVkQZ0WLXO+qF12k4YBWtwl/pJj9iB7v8X37Q4f/2h+xI9dMU4jGHd9QJ3n9auMXD6sVUUQbIIPihKh215/9oDaXjt+F0RdnVd3qm16gjtc9zeYbhCW4wItMzM5WBQcbA5O/tKoa+e/qKqwMZDwcyiATHvRc/GvSCoKGvcF83VHQmsQF7zf0f84N+G6/T8bycxq88f73DDzWF2+x51k0HgluTWHBD7xrq7MgXnK8knmOEV1dTuv6Bx0hhYiRPdextBlegV/+24l7IfEm04XI56nHfZSZ6fP12jEaLHnHPxjvoZp34Kk6iI47LxRv1HQOC4IpYMMHpkx07SrCDCIFduaOB5+56NHd3uUdTO/1eaMivq8a1pnpBQ/x+AER4Df2O0vdaYUtVqs8wjAXUnZ7iRasjXneyKlX1Ylmv2bYrUc3UAKbqbgGemli0wCUUCS6MNr2qgccC3sGI2Qy4hS84FtXqI/8e+224U0emeVVIFWVsxNXYZFit2ItbcH1SeT8bw+CubkzBFRxTl9W+XjcIR+8a1qFfjdpBUw+AInxv5ju4wMOx2NQcqaSrzHMZpJrMROcVbsevUg1tN4hSlV6/3ze4k+sreyxFO+VZEhvj4YAIvasM9niIyptiWpdLyO9n4wz5wV5P6WusEjX7Jh/lH/VYxh4Tpk47vpXXO6L5R6fcD7cxgkNjUoSqaI7lOqTYPNZUNc11Wf1oD5WhqjkzcoeKvlBmLjmdvk39iUtzZ0Ta2NBZkrHYcTVNs8m0yDV001B7R+ny66UKrVRTCLgL9Hx/thtPbzpt4OE2mdIX1vY73uZezLERlP5KnKJCZFQNwWUgcd3yYa05lVW99s5xOqfRlL5ubaX0hRv2vUChbR0iMh4OZFDFddP09nVxQnBrguu2HjKCa9/PEJwqEBwAp00Df4XojQC1ItO8KqSKMk55THmbYZfOFU2sHE4JG0NkAlcrQtNUD+FYgaktDJHgXFOfe5PRh1bB2jp1OE8n3Q2qcE7VFAIEGN66U05/J53n089b+9fxKrFhz8xvEKpmcg7x3WNjqtT8y9FmFszzGJqm7dLk0F+8sAbKVCvckgSVDPCJ9q/pJHa1SSDh4XLgaw/ro+hspKiiDHi4DNIXdrZDPAhgjKIga6hnx1ujUfYlVQN79j7/IJqPHmDoCVeXPN4eKlF1GwmuW7xDV7CO2dsxxKtdO96yLD7B3FtWs+OZ9hVlwGf99rnkPA+3pwQT1P0GYZ+XyRiMSPMK0PnUYO9DvDnCgyERSzc7rJTdbN1uNBDcmpwQnH8p+0qP72Az4vuF8MnlfEUDBSsf+tyUiWxnh1lw5xVUqb7gOkrPYG/iguuo3v4RmrdLicoL5e9kBWo7Pc+Y2fb3mHaT6/m3BAhuTU4KzmRS8J3Znu+n+Mwjvw23LDjy/GB07Atux5fdHveC5K1y8HL8N/LyNM9xql7ZHe9Tt3IzEhnBbaHjPg24XobUn/G9HPhuDt3DtEXMYSTTEIt2eCfw2CHiMzJPdt4N+RJ/cljZwSJE9iENnbsJwUkxIZcp4Ji1s+bEFwnm9eY66WhiOPEWsNKCRR47e2aGH7DIi4ItT3fNBoKTYcjdkuF1yzGcfq7gKNqIOmAaVdKkHt5RTe1Er14wmnHS9TUPCE4G7yoxkVkezpHvkESkr2k37twd0M50ZqVdINOjN+7KQ4Z0VkWkz+nU2JgT2STS/mvotdjMRH23Us13FC/emncz16cGRSt+G+9u4J6sx1enqAyZfV0huGJUl7pDbyORqKviOO6YNdLeTl/eI474hl7BVsGsek3ck/j+jX0fqFFXOFWgKUBwRei77nFvxnzcok8zpjttyDdYdTqq6gume6ej66NYPJp6osJlgcbBHh9kYK7M1PSOJ7oea+uFV58VbZK2N0s8+aapbOUQSi3opk0WC0vdWb975EmkQ86Cve7NyDUc1sDXrA47tlzbdVXF1S3SbdrtTh1j7hqGvqCuaw5n7K/jjFRrpiiL4cwibqxZOzPqzUnp8c01dcVhke3cNMb8wTeGsdB1MlyHdotmNq1CUW0oRSWFgOrw+2w3koy+mNWpogxUqaBWILhKqcIHbC8Q05qsK5vCOXANp+Vff33WvG58xlF6llurkBGchEn7UNYLD70BhDYv1VJOdJkDKfArXA8ZD4d2HqgVCA7UCgTXDnCfN5kG3h2Z5lUhVZTRwGtbnsaPoJ8iEByoFQgO1AoEB2oFggO1AsEBcNpUEf63EZm+F3g4UCsQHKgVCA7UCgQHagWCA7UCwYFageDagUyPRSFVlIGV9+2gCq1UUgg8HKgVCA7UCgQHagWCA7UCwQEAthMZ74VukQyqCP9bicSFkxElAJVxYgd3QN4vNdiWJfjNLr1S/ecpBFmKv6hwyTDrRZZdOcPi8wheLb3IsZMuMO+EC4CHW53l53fILmKVtZM3rPw81itQ3hAAAADYfjZmtgjacKBWIDhQKxAcqBUIDtQKBAdqBYIDAAAAAAAAAAAAAACAasBsEdBOIDhQKxAcqBUIDtQKBAdqBYIDAAAAAAAAAAAAAA1mYzae3JgTAXeXKmZ6VFEGOn5BrUBwoFaq8JJgs9mZEc3JZEfW8h6XK9Dhf+a8DJot56wI9vhtPsqc/+V/1ncv7oL/5WUYqZwVQZXafKI9eHfX3423m3EEQDZKQBnvMgjKGKQzVqTMOYDtooSDi3c4X7sVGADBtYdxOmEVwsY+Gv2gkD2/NizXye/XqWVrVIw0tAHbr0vL1KhMKV5lqlrp9BVBldoCyra7PPy6FDUqkMDgtaHXa1uCXVbGbjpxZeDh2oDn4sp2oPEauVytDFqDWTpmYPSUXjppdeDhWgHzbkbplpzpjceWBIJrBeMq7rSDGhXIYirDdNLqVFCjVqB7sA10+0fppNXx5iiVZP0JK3efTT434BM8mVweeDhQK5vdc/ztdEKanxSaFFvImaz8U75LKDInu59OOsGtwlJ4OXvppDSfpxMKgYcDtQLBgVqB4ECtQHCgViA4UCsQHKgVCA7UCgQHagWCA7UCwYFageBArUBwoFYgOFArEByoFQgO1AoEB2oFgmsP1dzrkovvqzkJsA2U3vnIo+RSwc2eYg4qwOy8Exw9ak2Wclah63ziH3x+r7/f75pAcE2n97Po8B36xrqLBY2Po8NP6IESWxtCcE3HoHPBJg8fPkRr7+5m0xnF3w1HcUu1wyC4prMgM3RIDk2XslZAI9fya9LRYSprNcqIFWwDs3J7+wZ0q9nVEIJrPsML4dEjyeQVGd8THpVzcBBc85mEOxHeWEpeETXsf0M/HMjn5s/9f8ttSPjpL3e8f3d+mcpYEQQNjecCOe6YFPP9dMZK3EO/eoCVMvhVOmNFILimc0SO0e8u1HJ6G9Dhg5Nd9/g2nV23K88Hgms6DpFFrFY956glRghYqHCosNr0TP8gnbUaEFzT2aHDf6NzvUl/VkJvrK3vDm6fcVSj7KZ9CBoaj/tvdN46MMn+RTpnFfQ/3CbbcWlcchtMCK7xvENfnTGh3Cnnmz6hM19yyZ7ov0vnrAYE13zO+4ME3nOh1+es7bXepv8+nbEaEFzjOR8+pP7cUvKKnD0K9gAt98h7CK7xLKJYoUzQoJWrkGMguKYzix4fU+YxC92qhIJukaYzfDs63I9TV2X8WXRYuNN0LlUJFwAp4OEazzeOg4NhmUGCB0LXVGJdBAeCazxquM6qVHw5CUsp+cwuVKlN5yDya2WG751odeDayyJ8ILimo8a3uMQzoUej6LDcDExUqU0njlIfLTE7HFEqWJlyrf2QM+mE1YCHazyPBoMEx6VaX/cG7zZKhR4QXAso2Y8R4AZRQ0m9QXCNp0znW0wcpZYEbbim0+uU7DmrFni4pmO8TX8crr3vvL/ugOoosQB6NC9Rr8LDNZ/z7/WDo9n+15Zy5Bknnoh96JbYaQ6CawEXboSKW383myTd38TdwKuCKrUFHDxy45FwBH9dbieOP6eEv1sReLg2oDx2Y9evBstMwkxw20inyAIP1wbGg8euP7pzQIOZte74VtjxG9Bdd746BNcKuOKIzr1H9HA6SxJzedHXmrKF4NrCmPYHFp135+suFizbBgyB4JrO4UPBWGqpjTCrGmiA4BqP61bSFVIVEFwbUEaVDKmqFTg6CK4NKG+vOaK1zGclJ19y0A8HagWCA7WyPYLTo87tnySTk8TtA5FJXIiEichig5D4PjJUU4oUW9SG+7t0wkmKL5dEIRImm0M1J1tNKVJskeDA2pjnS08N55ylchtKc7ZIcH8exuS6sGPpvwT/aorIJCpEwkT8OZuDxPeh4klJcqVUILetEpwTDh9H+0+dILQQb30cFSJhIv6czUHi+zAZDZy5Ns2ZUiRVijKybdvKKUWK7RFc3OcofMxYsUmi47LYRGSxQUh8HxosPvD+fexOKiNG4jtrxsd/4P/eNxeaSLE9glPNYPxY/ZvvCH5m4bYt6o9JYBIX8mP69nJWiMTnbA4S36f7Hn3VYBWl7vSFA/DduBTBd979pf+Iho79+3Kdv9sjOFL8yYPO3/BnxGYTWQhN4kKESHzO5lD8fUw6t+BOyToUT5pMlCL4zod0ZsA95KxFT6J5KTz4vrBh++PwQGgSFSJhIrTYIIq/j0V6MFny5tcEJhKldEkJamRtV1w1S7BFgvte0HjQ/+el5YyYIErV/1poEhUiYSK02CCKv48dL0AQzoUrLkWlfqCzO+svoOFskeDsIJSyXhBObw4tfhA8LvskUSESJuLP2Rwkvk9hp4hcKaFfy4ljZdieoS07io6E/eJx/CQyiQuRMBFZbBAS3yd6VNFA3M9TXEoUb2gl9zPfIg8XE20iKsKSMSmmsJANQvh95uds72v035NwdcILN1J73jKG/kclpyhtj+A0zf8dKuYPFUGgFDzzWDFfEpnEhbz0Q8GUa4nP2Rwkvk+osuP9ZOoyZlyK4DvfCZbNHJeT2zYJjqLI/y8El5bo78MDV2QSdx9kh/8k9Tmbg8T3YW7JNtWpuxBbJEoRf+eRbWmaNROXIsUWCe6HgbNXhP2X9AP/H+W/C02iQiRMhBYbhMT3Ucwb/sH+UnISmVIGv/YPvlhcMeexRYJzizfWCy0EsT0lCpEwKY064PXQ0B/09hpHfgvJTxmNs5tLKyHxfXau02NTcm5SUP1mIFFK9zafLTL9lD4uV6mWCznq5NXiU301nXACmUKKTWTRj3jnvm3zDdoMnZWrehMmh15saRxV8GOXONne9XP7d2azxf6FX6SzIopLMX575st7R0fW3j3pnBUp/qhN4VJ2azbJc1fSKWlYIWqBETPRf2JWcl00r9NqOuVz0RT+PD7vD82mvFZSwhinDBIXxQhrsQPxtvl+KYoivjR2ONLgnG3Ltvl6IvLX9Mz5hOrRT74bVhta/y8zRrO1f/ie/VxQk2nmj+m/Cm6Y3vnLH7mCPHkmXt++6z030uvlt72qy+9bnVexq7j2D/Qj5YC5sWBQL2Po3ToX/XiEIw1kBP1vfMp05jXpngnr/5lQtlJsj+CSXdw9N3ui4A/ox8OgGWIfZo1DKzSPEu3Jd/9BFJPNre9Hc8Q2GYX+m7fgVPlzruJ/TOV66Dce8Q8G4kCW6e176oL9BCeG+lKmLKef3e/HE1rJ32ElVUc9JH4bR3Mte+Na7c//NK4WMgb9ej/0L/pPvMJ6OV9/+k9utIvf5tILFjjPyWWkMn3mdIM1HweqpjyabcD4uz//zmxiWc7Umoz/299mVfUj+qjbIW200/1UWIoU4iu+cTh6TPelTA9HRxbFisv6qfoVg+qvUzrKW0l+af5XprhNsyFYQUeQ+7fL6QmmD9O/fnjrvQ9/8a5wmJTI9FTk8LGvgz/LqvXufJE+/t3nv//1r36TzlmRrMI3lOUpX3+f0UJjOP8Ue/yMdt7EH1l0/R9atpeM+I7yp3a2rjeGRTBSmjeiPr/gP/woZ2e4H/lZyku8/Wb9Y9gsSTK9R/2U/3uvknFZV2CLPJwcl75LeiCkLN//0g7/q9FL3CizX0pVFIUFbPxvx/rrUYEoTxn9Jb/+UwNPl8nBQxcuXPjqQ2K9keN/S9MLV116djnXx3EfvOeeLz1YZgdzzhZ5uB/IRXX2d/+efjQhRc2yt8m9comu/Olf2H/zw7714+9lXD2Vj49Fkyb+8oeJrM3jf5L2GnNHxt/+WToniVO0k81L3kTCK3/qvRC1I1gYVsGWOFskuJwmyBL2t3WHiSZzMNsd/iUP/a0ZfX/xt9/7i6yBHOc7pP/dn0UhapbJ5nBpuDjiXRn+N/kB/Y+TleGOG9a3b+8n05P8xV95/1i8RlWOsqoGVY+uSE67V4LtEVxm/bdMeGkt69uqYIDq0pX/ZP/Nd2d8EsUww72Rdz1dEkR8m8eR+n3lxz9a+D+LSebQ1DvphJMcf197iejP/hd/++A7WYKjT6KjckNb2yO4rKuQwo7dkSOyv3RM3/a1m3VzfNwfZAW4m4kzpW/ntM6S5KwTnNJ/dmnhXZFy3WyFbI/gasQfEGgKD4e/ntxfUVFFGU0SGZZTZOOiVJBiEfXjZnXoyiKeabIiEFzTmUTu+t0Sj8haRCGbU0a3qFKbz87M14pqlpktoPb9gGyuffSgZJMxGwiu8bwfHuQEDcXcjo5K6Q1VaovI7gaqGXi4xvPIseoHoXkDroXcf7zreM4t9QykVYHgGo8ednmUutfTcOl9ScGhSm06B9H4p8SQgxAn6qcTdKjLAsE1nR1vfoxHicnhaiyUtqxpAGviz4ZjnC8zue+z8ODzcmOp8HDtQXa6TT73pRNWAx6u8ZwPhOYWDZfmcjaIcY9K9B5zILjG42+eVJZFRb14EFzTOYzCyoxlk9K4FekNbbjGM4q2ISzjXJQSEe4SZU4CbAVuuAfm+/vJ5BWxotVEmA8H5AhW4JekTN8KwcO1gF441FBqIpsTOjbM+AW5HEUCKbPEdlBi8uYSEFzTiRegXS8xXSSWrVWiFILgms8o3tOnhFSU6L3BKv11QRuu8bj+6L2lkVmiwR8shO5an3w5lbMaEFzjmar0Pt+U0P5KCb2R02cldfn+I2UmqkNwLUCZ0b43nLpTZmcQnTfi+H/3lRtMRRuu6cTrUssoJV6XWqpzBYJrPt4O1h43k8krYlW0lw0E13gSUWqJmx1HqW7GTrYrgDZc45n5OusopZYjBAuhp8ZHX0jlrAYE13iihdCPlNnrLl4IXUa2EFwLiHRWRm8U7JpfmhLVOtgKjku28n0Mb9v3CoDgmo4TLVwuM4fSrES2BME1n2E09vnzZPKKjLN2TF4HtOEaTzCW6jFw123JJTeV1jrrlgLBtYDkDg/a27SfeLkCyU2l3Y/oj9Zd5ArBNZ5oj1/GwX58vBrJB0E7e+svqkYbrvGUiRViyu2ZFAMP13TiKSJl1qXGQWqZUggerk0kgocSlJvwCw/XfPTuu8HRI5P1vVNHn3lPE6TP7y9RCgTXfNR/jQ5vrD+cOvp1dPhRqWEuCK7psCrwgn/09oX1g8sx0f1+nNqdrl8KQXDN54j2w7ihxBTzwWd7gVvTnFJzMBE0NB3nfLiG+aGl9NU4PBtOLS9RnXLg4RrPLNxpq4xjirewKfnQLXg4UCsQHKgVCA7UCgQHagWCA7UCwYFageBArUBwoFYgOFArEByoFQgO1AoEB2oFggO1AsGBWoHgQK1AcKBWSjwr4q6zyecGfFZewAUPBwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGgQ/x82GW1J4tsk8wAAAABJRU5ErkJggg==>

[image4]: <data:image/png;base64,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>
