# Pulsar Improvement Proposal (PIP)

## What is a PIP?

The PIP is a "Pulsar Improvement Proposal" and it's the mechanism used to propose changes to the Apache Pulsar codebases.

The changes might be in terms of new features, large code refactoring, changes to APIs.

In practical terms, the PIP defines a process in which developers can submit a design doc, receive feedback and get the "go ahead" to execute.

### What is the goal of a PIP?

There are several goals for the PIP process:

1. Ensure community technical discussion of major changes to the Apache Pulsar codebase.

2. Provide clear and thorough design documentation of the proposed changes. Make sure every Pulsar developer will have enough context to effectively perform a code review of the Pull Requests.

3. Use the PIP document to serve as the baseline on which to create the documentation for the new feature.

4. Have greater scrutiny to changes are affecting the public APIs (as defined below) to reduce chances of introducing breaking changes or APIs that are not expressing an ideal semantic.

It is not a goal for PIP to add undue process or slow-down the development.

### When is a PIP required?

* Any new feature for Pulsar brokers or client
* Any change to the public APIs (Client APIs, REST APIs, Plugin APIs)
* Any change to the wire protocol APIs
* Any change to the API of Pulsar CLI tools (eg: new options)
* Any change to the semantic of existing functionality, even when current behavior is incorrect.
* Any large code change that will touch multiple components
* Any changes to the metrics (metrics endpoint, topic stats, topics internal stats, broker stats, etc.)
* Any change to the configuration

### When is a PIP *not* required?

* Bug-fixes
* Simple enhancements that won't affect the APIs or the semantic
* Small documentation changes
* Small website changes
* Build scripts changes (except: a complete rewrite)

### Who can create a PIP?

Any person willing to contribute to the Apache Pulsar project is welcome to create a PIP.

## How does the PIP process work?

A PIP proposal can be in these states:
1. **DRAFT**: (Optional) This might be used for contributors to collaborate and to seek feedback on an incomplete version of the proposal.

2. **DISCUSSION**: The proposal has been submitted to the community for feedback and approval.

3. **ACCEPTED**: The proposal has been accepted by the Pulsar project.

4. **REJECTED**: The proposal has not been accepted by the Pulsar project.

5. **IMPLEMENTED**: The implementation of the proposed changes have been completed and everything has been merged.

6. **RELEASED**: The proposed changes have been included in an official
   Apache Pulsar release.


The process works in the following way:

1. Fork https://github.com/apache/pulsar repository (Using the fork button on GitHub).
2. Clone the repository, and on it, copy the file `pip/TEMPLATE.md` and name it `pip-xxx.md`. The number `xxx` should be the next sequential number after the last contributed PIP. You view the list of contributed PIPs (at any status) as a list of Pull Requests having a "PIP" label. Use the link [here](https://github.com/apache/pulsar/pulls?q=is%3Apr+label%3APIP+) as shortcut.
3. Write the proposal following the section outlined by the template and the explanation for each section in the comment it contains (you can delete the comment once done).
   * If you need diagrams, avoid attaching large files. You can use [MermaidJS](https://mermaid.js.org/) as simple language to describe many types of diagrams. 
4. Create GitHub Pull request (PR). The PR title should be `[improve][pip] PIP-xxx: {title}`, where the `xxx` match the number given in previous step (file-name). Replace `{title}` with a short title to your proposal.
   *Validate* again that your number does not collide, by step (2) numbering check. 
5. The author(s) will email the dev@pulsar.apache.org mailing list to kick off a discussion, using subject prefix `[DISCUSS] PIP-xxx: {PIP TITLE}`. The discussion will happen in broader context either on the mailing list or as general comments on the PR. Many of the discussion items will be on particular aspect of the proposal, hence they should be as comments in the PR to specific lines in the proposal file.
6. Update file with a link to the discussion on the mailing. You can obtain it from [Apache Pony Mail](https://lists.apache.org/list.html?dev@pulsar.apache.org).
7. Based on the discussion and feedback, some changes might be applied by authors to the text of the proposal. They will be applied as extra commits, making it easier to track the changes.
8. Once some consensus is reached, there will be a vote to formally approve the proposal. The vote will be held on the dev@pulsar.apache.org mailing list, by
   sending a message using subject `[VOTE] PIP-xxx: {PIP TITLE}`. Make sure to include a link to the PIP PR in the body of the message.
   Make sure to update the PIP with a link to the vote. You can obtain it from [Apache Pony Mail](https://lists.apache.org/list.html?dev@pulsar.apache.org). 
   Everyone is welcome to vote on the proposal, though only the vote of the PMC members will be considered binding.
   The requirement is to have at least one binding +1 vote from a lazy majority if no binding -1 votes have been cast on the PIP.
   The vote should stay open for at least 48 hours.
9. When the vote is closed, if the outcome is positive, ask a PMC member (using voting thread on mailing list) to merge the PR.
10. If the outcome is negative, please close the PR (with a small comment that the close is a result of a vote).

All the future implementation Pull Requests that will be created, should always reference the PIP-XXX in the commit log message and the PR title.
It is advised to create a master GitHub issue to formulate the execution plan and track its progress.

### Example 
* Eve ran into some issues with the client metrics - she needed a metric which was missing.
* She read the code a bit, and has an idea what metrics she wishes to add.
* She summarized her idea and direction in an email to the DEV mailing list (she located it on
[Discussions]([url](https://pulsar.apache.org/community/#section-discussions)) section on the website.
* She didn't get any response from the community, so she joined the next
[community meeting]([url](https://github.com/apache/pulsar/wiki/Community-Meetings)). There Matteo Merli and Asaf helped 
setup a channel in Slack to brainstorm the idea and meet on Zoom with a few Pulsar contributors (e.g. Lari and Tison).
* Once Eve had a good enough context, and good design outline, she opened a new branch in her Pulsar repository, duplicated 
TEMPLATE.md and created pip-xxx.MD (the number she will take later).
* She followed the template and submitted the pip as a new PR to pulsar repository.
* Once the PR was created, she modified the version to match the rules described at step 2, both for PR title and file name.
* She sent an email to the DEV mailing list, titled "[DISCUSS] PIP-123: Adding metrics for ..." , described shortly in the
email what the PIP was about and gave a link.
* She got no response for anyone for 2 weeks, so she nudged the people that helped
  her brainstorm (e.g. Lary and Tison) and pinged in #dev that she needs more reviewers.
* Once she got 3 reviews from PMC members and the community had at least a few days from the moment
  the PR was announceed on DEV, she sent a vote email to the DEV mailing list titled
  "[VOTE] PIP-123: Adding metrics for ...".
* She nudged the reviewers to reply with a binding vote, waited for 2-3 days, and then
  concluded the vote by sending a reply tallying up the binding and non-binding votes. 
* She updated the PIP with links to discuss and vote emails, and then asked a PMC member
  who voted +1, to merge (using GitHub mentionon the PR).


## List of PIPs

### Current PIPs Table of Contents

The following table lists all current PIPs in this directory, sorted by PIP number:

| PIP Number | Title |
|------------|-------|
| PIP-1 | [Pulsar Proxy](pip-1.md) |
| PIP-2 | [Non Persistent topic](pip-2.md) |
| PIP-3 | [Message dispatch throttling](pip-3.md) |
| PIP-4 | [Pulsar End to End Encryption](pip-4.md) |
| PIP-5 | [Event time](pip-5.md) |
| PIP-6 | [Guaranteed Message Deduplication](pip-6.md) |
| PIP-7 | [Pulsar Failure domain and Anti affinity namespaces](pip-7.md) |
| PIP-8 | [Pulsar beyond 1M topics](pip-8.md) |
| PIP-9 | [Adding more Security checks to Pulsar Proxy](pip-9.md) |
| PIP-10 | [Remove cluster for namespace and topic names](pip-10.md) |
| PIP-11 | [Short topic names](pip-11.md) |
| PIP-12 | [Introduce builder for creating Producer Consumer Reader](pip-12.md) |
| PIP-13 | [Subscribe to topics represented by regular expressions](pip-13.md) |
| PIP-14 | [Topic compaction](pip-14.md) |
| PIP-15 | [Pulsar Functions](pip-15.md) |
| PIP-16 | [Pulsar "instance" terminology change](pip-16.md) |
| PIP-17 | [Tiered storage for Pulsar topics](pip-17.md) |
| PIP-18 | [Pulsar Replicator](pip-18.md) |
| PIP-19 | [Pulsar SQL](pip-19.md) |
| PIP-20 | [Mechanism to revoke TLS authentication](pip-20.md) |
| PIP-21 | [Pulsar Edge Component](pip-21.md) |
| PIP-22 | [Pulsar Dead Letter Topic](pip-22.md) |
| PIP-23 | [Message Tracing By Interceptors](pip-23.md) |
| PIP-24 | [Simplify memory settings](pip-24.md) |
| PIP-25 | [Token based authentication](pip-25.md) |
| PIP-26 | [Delayed Message Delivery](pip-26.md) |
| PIP-27 | [Add checklist in github pull request template](pip-27.md) |
| PIP-28 | [Pulsar Proxy Gateway Improvement](pip-28.md) |
| PIP-29 | [One package for both pulsar-client and pulsar-admin](pip-29.md) |
| PIP-30 | [change authentication provider API to support mutual authentication](pip-30.md) |
| PIP-31 | [Transaction Support](pip-31.md) |
| PIP-32 | [Go Function API, Instance and LocalRun](pip-32.md) |
| PIP-33 | [Replicated subscriptions](pip-33.md) |
| PIP-34 | [Add new subscribe type Key_shared](pip-34.md) |
| PIP-35 | [Improve topic lookup for topics that have high number of partitions](pip-35.md) |
| PIP-36 | [Max Message Size](pip-36.md) |
| PIP-37 | [Large message size handling in Pulsar](pip-37.md) |
| PIP-38 | [Batch Receiving Messages](pip-38.md) |
| PIP-39 | [Namespace Change Events](pip-39.md) |
| PIP-40 | [Pulsar Manager](pip-40.md) |
| PIP-41 | [Pluggable Protocol Handler](pip-41.md) |
| PIP-42 | [KoP - Kafka on Pulsar](pip-42.md) |
| PIP-43 | [producer send message with different schema](pip-43.md) |
| PIP-44 | [Separate schema compatibility checker for producer and consumer](pip-44.md) |
| PIP-45 | [Pluggable metadata interface](pip-45.md) |
| PIP-46 | [Next-gen Proxy](pip-46.md) |
| PIP-47 | [Time Based Release Plan](pip-47.md) |
| PIP-48 | [hierarchical admin api](pip-48.md) |
| PIP-49 | [Permission levels and inheritance](pip-49.md) |
| PIP-50 | [Package Management](pip-50.md) |
| PIP-51 | [Tenant policy support](pip-51.md) |
| PIP-52 | [Message dispatch throttling relative to publish rate](pip-52.md) |
| PIP-53 | [Contribute DotPulsar to Apache Pulsar](pip-53.md) |
| PIP-54 | [Support acknowledgement at batch index level](pip-54.md) |
| PIP-55 | [Refresh Authentication Credentials](pip-55.md) |
| PIP-56 | [Python3 Migration](pip-56.md) |
| PIP-57 | [Improve Broker's Zookeeper Session Timeout Handling](pip-57.md) |
| PIP-58 | [Support Consumers  Set Custom Retry Delay](pip-58.md) |
| PIP-59 | [gPRC Protocol Handler](pip-59.md) |
| PIP-60 | [Support Proxy server with SNI routing](pip-60.md) |
| PIP-61 | [Advertised multiple addresses](pip-61.md) |
| PIP-62 | [Move connectors, adapters and Pulsar Presto to separate repositories](pip-62.md) |
| PIP-63 | [Readonly Topic Ownership Support](pip-63.md) |
| PIP-64 | [Introduce REST endpoints for producing, consuming and reading messages](pip-64.md) |
| PIP-65 | [Adapting Pulsar IO Sources to support Batch Sources](pip-65.md) |
| PIP-66 | [Pulsar Function Mesh](pip-66.md) |
| PIP-67 | [Pulsarctl - An alternative tools of pulsar-admin](pip-67.md) |
| PIP-68 | [Exclusive Producer](pip-68.md) |
| PIP-69 | [Schema design for Go client](pip-69.md) |
| PIP-70 | [Introduce lightweight broker entry metadata](pip-70.md) |
| PIP-71 | [Pulsar SQL migrate SchemaHandle to presto decoder](pip-71.md) |
| PIP-72 | [Introduce Pulsar Interface Taxonomy: Audience and Stability Classification](pip-72.md) |
| PIP-73 | [Configurable data source priority for message reading](pip-73.md) |
| PIP-74 | [Pulsar client memory limits](pip-74.md) |
| PIP-75 | [Replace protobuf code generator](pip-75.md) |
| PIP-76 | [Streaming Offload](pip-76.md) |
| PIP-77 | [Contribute Supernova to Apache Pulsar](pip-77.md) |
| PIP-78 | [Generate Docs from Code Automatically](pip-78.md) |
| PIP-79 | [Reduce redundant producers from partitioned producer](pip-79.md) |
| PIP-80 | [Unified namespace-level admin API](pip-80.md) |
| PIP-81 | [Split the individual acknowledgments into multiple entries](pip-81.md) |
| PIP-82 | [Tenant and namespace level rate limiting](pip-82.md) |
| PIP-83 | [Pulsar client: Message consumption with pooled buffer](pip-83.md) |
| PIP-84 | [Pulsar client: Redeliver command add epoch](pip-84.md) |
| PIP-85 | [Expose Pulsar-Client via Function/Connector BaseContext](pip-85.md) |
| PIP-86 | [Pulsar Functions: Preload and release external resources](pip-86.md) |
| PIP-87 | [Upgrade Pulsar Website Framework (Docusaurus)](pip-87.md) |
| PIP-88 | [Replicate schemas across multiple](pip-88.md) |
| PIP-89 | [Structured document logging](pip-89.md) |
| PIP-90 | [Expose broker entry metadata to the client](pip-90.md) |
| PIP-91 | [Separate lookup timeout from operation timeout](pip-91.md) |
| PIP-92 | [Topic policy across multiple clusters](pip-92.md) |
| PIP-93 | [Transaction performance tools](pip-93.md) |
| PIP-94 | [Message converter at broker level](pip-94.md) |
| PIP-95 | [Smart Listener Selection with Multiple Bind Addresses](pip-95.md) |
| PIP-96 | [Message payload processor for Pulsar client](pip-96.md) |
| PIP-97 | [Asynchronous Authentication Provider](pip-97.md) |
| PIP-98 | [Redesign Pulsar Information Architecture](pip-98.md) |
| PIP-99 | [Pulsar Proxy Extensions](pip-99.md) |
| PIP-100 | [Pulsar pluggable topic factory](pip-100.md) |
| PIP-101 | [Add seek by index feature for consumer](pip-101.md) |
| PIP-104 | [Add new consumer type: TableView](pip-104.md) |
| PIP-105 | [Support pluggable entry filter in Dispatcher](pip-105.md) |
| PIP-106 | [Negative acknowledgment backoff](pip-106.md) |
| PIP-107 | [Introduce the chunk message ID PIP](pip-107.md) |
| PIP-108 | [Pulsar Feature Matrix (Client and Function)](pip-108.md) |
| PIP-109 | [Introduce Bot to Improve Efficiency of Developing Docs](pip-109.md) |
| PIP-110 | [Topic metadata](pip-110.md) |
| PIP-111 | [Add messages produced by Protocol Handler When checking maxMessagePublishBufferSizeInMB](pip-111.md) |
| PIP-112 | [Generate Release Notes Automatically](pip-112.md) |
| PIP-116 | [Create Pulsar Writing Style Guide](pip-116.md) |
| PIP-117 | [Change Pulsar Standalone defaults](pip-117.md) |
| PIP-118 | [Do not restart brokers when ZooKeeper session expires](pip-118.md) |
| PIP-119 | [Enable consistent hashing by default on KeyShared dispatcher](pip-119.md) |
| PIP-120 | [Enable client memory limit by default](pip-120.md) |
| PIP-121 | [Pulsar cluster level auto failover](pip-121.md) |
| PIP-122 | [Change loadBalancer default loadSheddingStrategy to ThresholdShedder](pip-122.md) |
| PIP-123 | [Introduce Pulsar metadata CLI tool](pip-123.md) |
| PIP-124 | [Create init subscription before sending message to DLQ](pip-124.md) |
| PIP-129 | [Introduce intermediate state for ledger deletion](pip-129.md) |
| PIP-130 | [Apply redelivery backoff policy for ack timeout](pip-130.md) |
| PIP-131 | [Resolve produce chunk messages failed when topic level maxMessageSize is set](pip-131.md) |
| PIP-132 | [Include message header size when check maxMessageSize for non-batch message on the client side](pip-132.md) |
| PIP-135 | [Include MetadataStore backend for Etcd](pip-135.md) |
| PIP-136 | [Sync Pulsar policies across multiple clouds](pip-136.md) |
| PIP-137 | [Pulsar Client Shared State API](pip-137.md) |
| PIP-143 | [Support split bundle by specified boundaries](pip-143.md) |
| PIP-144 | [Making SchemaRegistry implementation configurable](pip-144.md) |
| PIP-146 | [ManagedCursorInfo compression](pip-146.md) |
| PIP-148 | [Create Pulsar client release notes](pip-148.md) |
| PIP-149 | [Making the REST Admin API fully async](pip-149.md) |
| PIP-152 | [Support subscription level dispatch rate limiter setting](pip-152.md) |
| PIP-154 | [Max active transaction limitation for transaction coordinator](pip-154.md) |
| PIP-155 | [Drop support for Python2](pip-155.md) |
| PIP-156 | [Build and Run Pulsar Server on Java 17](pip-156.md) |
| PIP-157 | [Bucketing topic metadata to allow more topics per namespace](pip-157.md) |
| PIP-160 | [Make transactions work more efficiently by aggregation operation for transaction log and pending ack store](pip-160.md) |
| PIP-161 | [Exclusive Producer: new mode ExclusiveWithFencing](pip-161.md) |
| PIP-162 | [LTS Releases](pip-162.md) |
| PIP-165 | [Auto release client useless connections](pip-165.md) |
| PIP-173 | [Create a built-in Function implementing the most common basic transformations](pip-173.md) |
| PIP-174 | [Provide new implementation for broker dispatch cache](pip-174.md) |
| PIP-175 | [Extend time based release process](pip-175.md) |
| PIP-176 | [Refactor Doc Bot](pip-176.md) |
| PIP-177 | [Add the classLoader field for SchemaDefinition](pip-177.md) |
| PIP-178 | [Multiple snapshots for transaction buffer](pip-178.md) |
| PIP-179 | [Support the admin API to check unknown request parameters](pip-179.md) |
| PIP-180 | [Shadow Topic, an alternative way to support readonly topic ownership](pip-180.md) |
| PIP-181 | [Pulsar Shell](pip-181.md) |
| PIP-182 | [Provide new load balance placement strategy implementation for ModularLoadManagerStrategy](pip-182.md) |
| PIP-183 | [Reduce unnecessary REST call in broker](pip-183.md) |
| PIP-184 | [Topic specific consumer priorityLevel](pip-184.md) |
| PIP-186 | [Introduce two phase deletion protocol based on system topic](pip-186.md) |
| PIP-187 | [Add API to analyse a subscription backlog and provide a accurate value](pip-187.md) |
| PIP-188 | [Cluster migration or Blue-Green cluster deployment support in Pulsar](pip-188.md) |
| PIP-189 | [No batching if only one message in batch](pip-189.md) |
| PIP-190 | [Simplify documentation release and maintenance strategy](pip-190.md) |
| PIP-191 | [Support batched message using entry filter](pip-191.md) |
| PIP-192 | [New Pulsar Broker Load Balancer](pip-192.md) |
| PIP-193 | [Sink preprocessing Function](pip-193.md) |
| PIP-194 | [Pulsar client: seek command add epoch](pip-194.md) |
| PIP-195 | [New bucket based delayed message tracker](pip-195.md) |
| PIP-198 | [Standardize PR Naming Convention using GitHub Actions](pip-198.md) |
| PIP-201 | [Extensions mechanism for Pulsar Admin CLI tools](pip-201.md) |
| PIP-204 | [Extensions for broker interceptor](pip-204.md) |
| PIP-205 | [Reactive Java client for Apache Pulsar](pip-205.md) |
| PIP-209 | [Separate C++/Python clients to own repositories](pip-209.md) |
| PIP-243 | [Register Jackson Java 8 support modules by default](pip-243.md) |
| PIP-249 | [Pulsar website redesign](pip-249.md) |
| PIP-259 | [Make the config httpMaxRequestHeaderSize of the pulsar web server to configurable](pip-259.md) |
| PIP-261 | [Restructure Getting Started section](pip-261.md) |
| PIP-264 | [Support OpenTelemetry metrics in Pulsar](pip-264.md) |
| PIP-265 | [PR-based system for managing and reviewing PIPs](pip-265.md) |
| PIP-275 | [Rename numWorkerThreadsForNonPersistentTopic to topicOrderedExecutorThreadNum](pip-275.md) |
| PIP-276 | [Add pulsar prefix to topic_load_times metric](pip-276.md) |
| PIP-277 | [Add current cluster marking to clusters list command](pip-277.md) |
| PIP-278 | [Pluggable topic compaction service](pip-278.md) |
| PIP-279 | [Support topic-level policies using TableView API](pip-279.md) |
| PIP-280 | [Refactor CLI Argument Parsing Logic for Measurement Units using JCommander's custom converter](pip-280.md) |
| PIP-281 | [Add notifyError method on PushSource](pip-281.md) |
| PIP-282 | [Add Key_Shared subscription initial position support](pip-282.md) |
| PIP-284 | [Migrate topic policies implementation to use TableView](pip-284.md) |
| PIP-286 | [Support get position based on timestamp with topic compaction](pip-286.md) |
| PIP-289 | [Secure Pulsar Connector Configuration](pip-289.md) |
| PIP-290 | [Support message encryption in WebSocket proxy](pip-290.md) |
| PIP-292 | [Enforce token expiration time in the Websockets plugin](pip-292.md) |
| PIP-293 | [Support reader to read compacted data](pip-293.md) |
| PIP-296 | [Add getLastMessageIds API for Reader](pip-296.md) |
| PIP-297 | [Support terminating Function & Connector with the fatal exception](pip-297.md) |
| PIP-298 | [Support read transaction buffer snapshot segments from earliest](pip-298.md) |
| PIP-299 | [Support setting max unacked messages at subscription level](pip-299.md) |
| PIP-300 | [Add RedeliverCount field to CommandAck](pip-300.md) |
| PIP-301 | [Separate load data storage from configuration metadata store](pip-301.md) |
| PIP-302 | [Support for TableView with strong read consistency](pip-302.md) |
| PIP-303 | [Support PartitionedTopicStats exclude publishers and subscriptions](pip-303.md) |
| PIP-305 | [Add OpAddEntry and pendingData statistics info in JVM metrics](pip-305.md) |
| PIP-306 | [Support subscribing multi topics for WebSocket](pip-306.md) |
| PIP-307 | [Optimize Bundle Unload(Transfer) Protocol for ExtensibleLoadManager](pip-307.md) |
| PIP-312 | [Use StateStoreProvider to manage state in Pulsar Functions endpoints](pip-312.md) |
| PIP-313 | [Support force unsubscribe using consumer api](pip-313.md) |
| PIP-315 | [Configurable max delay limit for delayed delivery](pip-315.md) |
| PIP-318 | [Don't retain null-key messages during topic compaction](pip-318.md) |
| PIP-320 | [OpenTelemetry Scaffolding](pip-320.md) |
| PIP-321 | [Split the responsibilities of namespace replication-clusters](pip-321.md) |
| PIP-322 | [Pulsar Rate Limiting Refactoring](pip-322.md) |
| PIP-323 | [Complete Backlog Quota Telemetry](pip-323.md) |
| PIP-324 | [Switch to Alpine Linux base Docker images](pip-324.md) |
| PIP-325 | [Support reading from transaction buffer for pending transaction](pip-325.md) |
| PIP-326 | [Create a BOM to ease dependency management](pip-326.md) |
| PIP-327 | [Support force topic loading for unrecoverable errors](pip-327.md) |
| PIP-329 | [Strategy for maintaining the latest tag to Pulsar docker images](pip-329.md) |
| PIP-330 | [getMessagesById gets all messages](pip-330.md) |
| PIP-335 | [Support Oxia metadata store plugin](pip-335.md) |
| PIP-337 | [SSL Factory Plugin to customize SSLContext/SSLEngine generation](pip-337.md) |
| PIP-339 | [Introducing the --log-topic Option for Pulsar Sinks and Sources](pip-339.md) |
| PIP-342 | [Support OpenTelemetry metrics in Pulsar client](pip-342.md) |
| PIP-343 | [Use picocli instead of jcommander](pip-343.md) |
| PIP-344 | [Correct the behavior of the public API pulsarClient.getPartitionsForTopic(topicName)](pip-344.md) |
| PIP-347 | [add role field in consumer's stat](pip-347.md) |
| PIP-348 | [Trigger offload on topic load stage](pip-348.md) |
| PIP-349 | [Add additionalSystemCursorNames ignore list for ttl check](pip-349.md) |
| PIP-350 | [Allow to disable the managedLedgerOffloadDeletionLagInMillis](pip-350.md) |
| PIP-351 | [Additional options for Pulsar-Test client to support KeyStore based TLS](pip-351.md) |
| PIP-352 | [Event time based topic compactor](pip-352.md) |
| PIP-353 | [Improve transaction message visibility for peek-messages cli](pip-353.md) |
| PIP-354 | [apply topK mechanism to ModularLoadManagerImpl](pip-354.md) |
| PIP-355 | [Enhancing Broker-Level Metrics for Pulsar](pip-355.md) |
| PIP-356 | [Support Geo-Replication starts at earliest position](pip-356.md) |
| PIP-357 | [Correct the conf name in load balance module.](pip-357.md) |
| PIP-358 | [let resource weight work for OverloadShedder, LeastLongTermMessageRate, ModularLoadManagerImpl.](pip-358.md) |
| PIP-359 | [Support custom message listener executor for specific subscription](pip-359.md) |
| PIP-360 | [Admin API to display Schema metadata](pip-360.md) |
| PIP-363 | [Add callback parameters to the method: `org.apache.pulsar.client.impl.SendCallback.sendComplete`.](pip-363.md) |
| PIP-364 | [Introduce a new load balance algorithm AvgShedder](pip-364.md) |
| PIP-366 | [Support to specify different config for Configuration and Local Metadata Store](pip-366.md) |
| PIP-367 | [Propose a Contributor Repository for Pulsar](pip-367.md) |
| PIP-368 | [Support lookup based on the lookup properties](pip-368.md) |
| PIP-369 | [Flag based selective unload on changing ns-isolation-policy](pip-369.md) |
| PIP-370 | [configurable remote topic creation in geo-replication](pip-370.md) |
| PIP-373 | [Add a topic's system prop that indicates whether users have published TXN messages in before.](pip-373.md) |
| PIP-374 | [Visibility of messages in receiverQueue for the consumers](pip-374.md) |
| PIP-376 | [Make Topic Policies Service Pluggable](pip-376.md) |
| PIP-378 | [Add ServiceUnitStateTableView abstraction (ExtensibleLoadMangerImpl only)](pip-378.md) |
| PIP-379 | [Key_Shared Draining Hashes for Improved Message Ordering](pip-379.md) |
| PIP-380 | [Support setting up specific namespaces to skipping the load-shedding](pip-380.md) |
| PIP-381 | [Handle large PositionInfo state](pip-381.md) |
| PIP-383 | [Support granting/revoking permissions for multiple topics](pip-383.md) |
| PIP-384 | [ManagedLedger interface decoupling](pip-384.md) |
| PIP-389 | [Add Producer config compressMinMsgBodySize to improve compression performance](pip-389.md) |
| PIP-391 | [Improve Batch Messages Acknowledgment](pip-391.md) |
| PIP-392 | [Add configuration to enable consistent hashing to select active consumer for partitioned topic](pip-392.md) |
| PIP-393 | [Improve performance of Negative Acknowledgement](pip-393.md) |
| PIP-395 | [Add Proxy configuration to support configurable response headers for http reverse-proxy](pip-395.md) |
| PIP-396 | [Align WindowFunction's WindowContext with BaseContext](pip-396.md) |
| PIP-399 | [Fix Metric Name for Delayed Queue](pip-399.md) |
| PIP-401 | [Support set batching configurations for Pulsar Functions&Sources](pip-401.md) |
| PIP-402 | [Role Anonymizer for Pulsar Logging](pip-402.md) |
| PIP-404 | [Introduce per-ledger properties](pip-404.md) |
| PIP-406 | [Introduce metrics related to dispatch throttled events](pip-406.md) |
| PIP-407 | [Add a newMessage API to create a message with a schema and transaction](pip-407.md) |
| PIP-409 | [support producer configuration for retry/dead letter topic producer](pip-409.md) |
| PIP-412 | [Support setting messagePayloadProcessor in Pulsar Functions and Sinks](pip-412.md) |
| PIP-414 | [Enforce topic consistency check](pip-414.md) |
| PIP-415 | [Support getting message ID by index](pip-415.md) |
| PIP-416 | [Add a new topic method to implement trigger offload by size threshold](pip-416.md) |
| PIP-420 | [Provides an ability for Pulsar clients to integrate with third-party schema registry service](pip-420.md) |
| PIP-421 | [Require Java 17 as the minimum for Pulsar Java client SDK](pip-421.md) |
| PIP-422 | [Support global topic-level policy: replicated clusters and new API to delete topic-level policies](pip-422.md) |
| PIP-425 | [Support connecting with next available endpoint for multi-endpoint serviceUrls](pip-425.md) |
| PIP-427 | [Align pulsar-admin Default for Mark-Delete Rate with Broker Configuration](pip-427.md) |
| PIP-428 | [Change TopicPoliciesService interface to fix consistency issues](pip-428.md) |
| PIP-429 | [Optimize Handling of Compacted Last Entry by Skipping Payload Buffer Parsing](pip-429.md) |
| PIP-430 | [Pulsar Broker cache improvements: refactoring eviction and adding a new cache strategy based on expected read count](pip-430.md) |
| PIP-431 | [Add Creation and Last Publish Timestamps to Topic Stats](pip-431.md) |
| PIP-432 | [Add isEncrypted field to EncryptionContext](pip-432.md) |
| PIP-433 | [Optimize the conflicts of the replication and automatic creation mechanisms, including the automatic creation of topics and schemas](pip-433.md) |
| PIP-435 | [Add `startTimestamp` and `endTimestamp` for consuming messages in client cli](pip-435.md) |
| PIP-436 | [Add decryptFailListener to Consumer](pip-436.md) |

### Additional Information

1. You can view all PIPs (besides the historical ones) as the list of Pull Requests having title starting with `[improve][pip] PIP-`. Here is the [link](https://github.com/apache/pulsar/pulls?q=is%3Apr+title%3A%22%5Bpip%5D%5Bdesign%5D+PIP-%22) for it. 
   - Merged PR means the PIP was accepted.
   - Closed PR means the PIP was rejected.
   - Open PR means the PIP was submitted and is in the process of discussion.
2. All PIP files in the `pip` folder follow the naming convention `pip-xxx.md` where `xxx` is the PIP number.