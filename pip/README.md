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

### Historical PIPs
You can the view list of PIPs previously managed by GitHub wiki or GitHub issues [here](https://github.com/apache/pulsar/wiki#pulsar-improvement-proposals)

### Current PIPs Table of Contents

The following table lists all current PIPs in this directory, sorted by PIP number:

| PIP Number | Title |
|------------|-------|
| 264 | [Support OpenTelemetry metrics in Pulsar](pip-264.md) |
| 275 | [Rename numWorkerThreadsForNonPersistentTopic to topicOrderedExecutorThreadNum](pip-275.md) |
| 276 | [Add pulsar prefix to topic_load_times metric](pip-276.md) |
| 277 | [Add current cluster marking to clusters list command](pip-277.md) |
| 278 | [Pluggable topic compaction service](pip-278.md) |
| 279 | [Support topic-level policies using TableView API](pip-279.md) |
| 280 | [Refactor CLI Argument Parsing Logic for Measurement Units using JCommander's custom converter](pip-280.md) |
| 281 | [Add notifyError method on PushSource](pip-281.md) |
| 282 | [Add Key_Shared subscription initial position support](pip-282.md) |
| 284 | [Migrate topic policies implementation to use TableView](pip-284.md) |
| 286 | [Support get position based on timestamp with topic compaction](pip-286.md) |
| 289 | [Secure Pulsar Connector Configuration](pip-289.md) |
| 290 | [Support message encryption in WebSocket proxy](pip-290.md) |
| 292 | [Enforce token expiration time in the Websockets plugin](pip-292.md) |
| 293 | [Support reader to read compacted data](pip-293.md) |
| 296 | [Add getLastMessageIds API for Reader](pip-296.md) |
| 297 | [Support terminating Function & Connector with the fatal exception](pip-297.md) |
| 298 | [Support read transaction buffer snapshot segments from earliest](pip-298.md) |
| 299 | [Support setting max unacked messages at subscription level](pip-299.md) |
| 300 | [Add RedeliverCount field to CommandAck](pip-300.md) |
| 301 | [Separate load data storage from configuration metadata store](pip-301.md) |
| 302 | [Support for TableView with strong read consistency](pip-302.md) |
| 303 | [Support PartitionedTopicStats exclude publishers and subscriptions](pip-303.md) |
| 305 | [Add OpAddEntry and pendingData statistics info in JVM metrics](pip-305.md) |
| 307 | [Support key value admin](pip-307.md) |
| 312 | [Use StateStoreProvider to manage state in Pulsar Functions endpoints](pip-312.md) |
| 313 | [Support force unsubscribe using consumer api](pip-313.md) |
| 315 | [Configurable max delay limit for delayed delivery](pip-315.md) |
| 318 | [Don't retain null-key messages during topic compaction](pip-318.md) |
| 320 | [OpenTelemetry Scaffolding](pip-320.md) |
| 321 | [Split the responsibilities of namespace replication-clusters](pip-321.md) |
| 322 | [Pulsar Rate Limiting Refactoring](pip-322.md) |
| 323 | [Complete Backlog Quota Telemetry](pip-323.md) |
| 324 | [Switch to Alpine Linux base Docker images](pip-324.md) |
| 325 | [Support reading from transaction buffer for pending transaction](pip-325.md) |
| 326 | [Create a BOM to ease dependency management](pip-326.md) |
| 327 | [Support force topic loading for unrecoverable errors](pip-327.md) |
| 329 | [Strategy for maintaining the latest tag to Pulsar docker images](pip-329.md) |
| 330 | [getMessagesById gets all messages](pip-330.md) |
| 335 | [Support Oxia metadata store plugin](pip-335.md) |
| 337 | [SSL Factory Plugin to customize SSLContext/SSLEngine generation](pip-337.md) |
| 339 | [Introducing the --log-topic Option for Pulsar Sinks and Sources](pip-339.md) |
| 342 | [Support OpenTelemetry metrics in Pulsar client](pip-342.md) |
| 343 | [Use picocli instead of jcommander](pip-343.md) |
| 344 | [Correct the behavior of the public API pulsarClient.getPartitionsForTopic(topicName)](pip-344.md) |
| 347 | [add role field in consumer's stat](pip-347.md) |
| 348 | [Trigger offload on topic load stage](pip-348.md) |
| 349 | [Add additionalSystemCursorNames ignore list for ttl check](pip-349.md) |
| 350 | [Allow to disable the managedLedgerOffloadDeletionLagInMillis](pip-350.md) |
| 351 | [Additional options for Pulsar-Test client to support KeyStore based TLS](pip-351.md) |
| 352 | [Event time based topic compactor](pip-352.md) |
| 353 | [Improve transaction message visibility for peek-messages cli](pip-353.md) |
| 354 | [apply topK mechanism to ModularLoadManagerImpl](pip-354.md) |
| 355 | [Enhancing Broker-Level Metrics for Pulsar](pip-355.md) |
| 356 | [Support Geo-Replication starts at earliest position](pip-356.md) |
| 357 | [Correct the conf name in load balance module.](pip-357.md) |
| 358 | [let resource weight work for OverloadShedder, LeastLongTermMessageRate, ModularLoadManagerImpl.](pip-358.md) |
| 359 | [Support custom message listener executor for specific subscription](pip-359.md) |
| 360 | [Admin API to display Schema metadata](pip-360.md) |
| 363 | [Add callback parameters to the method: `org.apache.pulsar.client.impl.SendCallback.sendComplete`.](pip-363.md) |
| 364 | [Introduce a new load balance algorithm AvgShedder](pip-364.md) |
| 366 | [Support to specify different config for Configuration and Local Metadata Store](pip-366.md) |
| 367 | [Propose a Contributor Repository for Pulsar](pip-367.md) |
| 368 | [Support lookup based on the lookup properties](pip-368.md) |
| 369 | [Flag based selective unload on changing ns-isolation-policy](pip-369.md) |
| 370 | [configurable remote topic creation in geo-replication](pip-370.md) |
| 373 | [Add a topic's system prop that indicates whether users have published TXN messages in before.](pip-373.md) |
| 374 | [Visibility of messages in receiverQueue for the consumers](pip-374.md) |
| 376 | [Make Topic Policies Service Pluggable](pip-376.md) |
| 378 | [Add ServiceUnitStateTableView abstraction (ExtensibleLoadMangerImpl only)](pip-378.md) |
| 379 | [Key_Shared Draining Hashes for Improved Message Ordering](pip-379.md) |
| 380 | [Support setting up specific namespaces to skipping the load-shedding](pip-380.md) |
| 381 | [Handle large PositionInfo state](pip-381.md) |
| 383 | [Support granting/revoking permissions for multiple topics](pip-383.md) |
| 384 | [ManagedLedger interface decoupling](pip-384.md) |
| 389 | [Add Producer config compressMinMsgBodySize to improve compression performance](pip-389.md) |
| 391 | [Improve Batch Messages Acknowledgment](pip-391.md) |
| 392 | [Add configuration to enable consistent hashing to select active consumer for partitioned topic](pip-392.md) |
| 393 | [Improve performance of Negative Acknowledgement](pip-393.md) |
| 395 | [Add Proxy configuration to support configurable response headers for http reverse-proxy](pip-395.md) |
| 396 | [Align WindowFunction's WindowContext with BaseContext](pip-396.md) |
| 399 | [Fix Metric Name for Delayed Queue](pip-399.md) |
| 401 | [Support set batching configurations for Pulsar Functions&Sources](pip-401.md) |
| 402 | [Role Anonymizer for Pulsar Logging](pip-402.md) |
| 404 | [Introduce per-ledger properties](pip-404.md) |
| 406 | [Introduce metrics related to dispatch throttled events](pip-406.md) |
| 407 | [Add a newMessage API to create a message with a schema and transaction](pip-407.md) |
| 409 | [support producer configuration for retry/dead letter topic producer](pip-409.md) |
| 412 | [Support setting messagePayloadProcessor in Pulsar Functions and Sinks](pip-412.md) |
| 414 | [Enforce topic consistency check](pip-414.md) |
| 415 | [Support getting message ID by index](pip-415.md) |
| 416 | [Add a new topic method to implement trigger offload by size threshold](pip-416.md) |
| 420 | [Provides an ability for Pulsar clients to integrate with third-party schema registry service](pip-420.md) |
| 421 | [Require Java 17 as the minimum for Pulsar Java client SDK](pip-421.md) |
| 422 | [Support global topic-level policy: replicated clusters and new API to delete topic-level policies](pip-422.md) |
| 425 | [Support connecting with next available endpoint for multi-endpoint serviceUrls](pip-425.md) |
| 427 | [Align pulsar-admin Default for Mark-Delete Rate with Broker Configuration](pip-427.md) |
| 428 | [Change TopicPoliciesService interface to fix consistency issues](pip-428.md) |
| 429 | [Optimize Handling of Compacted Last Entry by Skipping Payload Buffer Parsing](pip-429.md) |
| 430 | [Pulsar Broker cache improvements: refactoring eviction and adding a new cache strategy based on expected read count](pip-430.md) |
| 431 | [Add Creation and Last Publish Timestamps to Topic Stats](pip-431.md) |
| 432 | [Add isEncrypted field to EncryptionContext](pip-432.md) |
| 433 | [Optimize the conflicts of the replication and automatic creation mechanisms, including the automatic creation of topics and schemas](pip-433.md) |
| 435 | [Add `startTimestamp` and `endTimestamp` for consuming messages in client cli](pip-435.md) |
| 436 | [Add decryptFailListener to Consumer](pip-436.md) |

### Additional Information
1. You can view all PIPs (besides the historical ones) as the list of Pull Requests having title starting with `[improve][pip] PIP-`. Here is the [link](https://github.com/apache/pulsar/pulls?q=is%3Apr+title%3A%22%5Bpip%5D%5Bdesign%5D+PIP-%22) for it. 
   - Merged PR means the PIP was accepted.
   - Closed PR means the PIP was rejected.
   - Open PR means the PIP was submitted and is in the process of discussion.
2. All PIP files in the `pip` folder follow the naming convention `pip-xxx.md` where `xxx` is the PIP number.
3. Note: There is also a duplicate file `pip-307-duplicate.md` which contains different content from the main `pip-307.md`.
