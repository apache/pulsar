# Pulsar Pull Request Naming Convention Guide

> 👩🏻‍🏫 **Summary**
> 
> This guide explains why you need good PR titles and how you do that with various self​-explanatory examples. 

**TOC**

<!-- TOC -->

- [Pulsar Pull Request Naming Convention Guide](#pulsar-pull-request-naming-convention-guide)
  - [Why do PR titles matter?](#why-do-pr-titles-matter)
  - [How to write good PR titles?](#how-to-write-good-pr-titles)
    - [💡Quick examples](#💡quick-examples)
    - [`type`](#type)
    - [`scope`](#scope)
      - [Pulsar](#pulsar)
      - [Client](#client)
    - [`Summary`](#summary)
    - [Full examples](#full-examples)
  - [References](#references)

<!-- /TOC -->

## Why do PR titles matter?

Engineers and writers submit or review PRs almost every day. 

A PR title is a summary of your changes. 

* Vague, boring, and unclear PR titles decrease team efficiency and productivity. 
  
* PR titles should be engaging, easy to understand, and readable. 

Good titles often bring many benefits, including but not limited to the following:

* Speed up the review process. 

    You can tell from the title what changes the PR introduces.

* Facilitate understanding of PR changes. 
  
    * PR titles are shown on Pulsar release notes as items. Concise PR titles make your changes easier to understand.
  
    * Especially when you read commit logs in command-line tools, clear commit messages show PR changes quickly.
  
* Increase search efficiency. 

    You can skim through hundreds of commits and locate desired information quickly.

* Remind you to think about your PR.

    If you can not write a PR title in a simple way (for example, [[type](#type)] [[scope](#scope)] [summary](#summary)), or you need to use several types / scopes, consider whether your PR contains **too many** changes across various scopes. If so, consider splitting this big PR into several small PRs. In this way, you might get your PRs reviewed faster.

## How to write good PR titles?

A PR title should be structured as follows: 

![alt_text](assets/naming-1.png)


> 💡 **Rule**
>
> A good title = clear format ([type](#type) and [scope](#scope)) + self-explanatory [summary](#summary)


### 💡Quick examples

Here are some examples of unclear and good PR titles for your quick reference. Good PR titles are concise and self-explanatory since they tell you the changes in a clear and direct way. 

For more examples with correct formats, see [Full examples](#full-examples).

🙌 **Examples**

Vague ❌|Clear ✅
|---|---
Producer getting producer busy is removing existing producer from list|[fix][broker] ​​Active producers with the same name are no longer removed from the topic map
Forbid to read other topic's data in managedLedger layer|[improve][broker] Consumers are not allowed to read data on topics to which they are not subscribed 
Fix kinesis sink backoff class not found|[improve][connector] xx connectors can now use the Kinesis Backoff class
K8s Function Name Length Check Allows Invalid StatefulSet |[improve][function] Function name length cannot exceed 52 characters when using Kubernetes runtime 

> 💡 **Steps**
>
> How to write a good PR title?
> 
> 1. Select a [type](#type).
> 
> 2. Select a  [scope](#scope).
> 
> 3. Write a [summary](#summary).

### `type`

`type` is "what actions do you take". 

It must be one of the following.

type|Pulsar PR label|What actions do you take? 
|---|---|---
cleanup| [type/cleanup](https://github.com/apache/pulsar/labels/type%2Fcleanup)|Remove unused code or doc.
improve|[type/improvement](https://github.com/apache/pulsar/labels/type%2Fimprovement)|Submit enhancements that are neither new features nor bug fixes.
feat|[type/feature](https://github.com/apache/pulsar/labels/type%2Ffeature)|Submit new features.
fix|[type/fix](https://github.com/apache/pulsar/labels/type%2Ffix)|Submit bug fixes.
refactor|[type/refactor](https://github.com/apache/pulsar/labels/type%2Frefactor)|Restructure existing code while preserving its external behavior. 
revert|To be created|Revert changes

>❗️ **Note**
>
> - Choose correct labels for your PR so that your PR will automatically go to the correct chapter in release notes. If you do not specify a type label, the PR might go to the wrong place or not be included in the release notes at all. 
> 
> - For more information about release note automation for Pulsar and clients, see [PIP 112: Generate Release Notes Automatically](https://docs.google.com/document/d/1Ul2qIChDe8QDlDwJBICq1VviYZhdk1djKJJC5wXAGsI/edit).

### `scope`

`scope` is "where do you make changes".

Pulsar and clients have separate release notes, so they have different scopes.

>❗️ **Note**
>
> If your PR affects several scopes, do not choose several scope labels at the same time since different scopes go to different chapters in release notes. Instead, choose the most affected label (scope), or else your PR goes to several chapters in release notes, which causes redundancies. Choose only one label as much as possible.

#### Pulsar

`scope` and PR labels must be one of the following.

scope |Pulsar PR label|Where do you make changes?
|---|---|---
admin|- scope/admin <br>- scope/topic-policy | - pulsar-admin <br> - REST API <br> - Java admin API
broker | - scope/broker | It’s difficult to maintain an exhaustive list since many changes belong to brokers. <br><br> Here just lists some frequently updated areas, it includes but not limited to:<br>  - key_shared <br> - replication <br> - metadata <br> - compaction
cli|- scope/tool| Pulsar CLI tools. <br> It includes: <br> - pulsar <br> - pulsar-client <br> - pulsar-daemon <br> - pulsar-perf <br> - bookkeeper<br> - broker-tool
io<br>(connector)|- scope/connector <br> - scope/connect <br> - scope/kafka|Connector
fn<br>(function)| - scope/function|Function
meta<br>(metadata)|- scope/zookeepeer|Metadata
monitor|- scope/metrics - scope/stats|Monitoring
proxy| - scope/proxy| Proxy
schema| - scope/schema <br> - scope/schemaregistry|Schema
sec<br>(security)| - scope/security <br> - scope/authentication <br> - scope/authorization|Security
sql|- scope/sql|Pulsar SQL
storage| - scope/bookkeeper storage|Managed ledge
offload<br>(tiered storage)|- scope/tieredstorage|Tiered storage
txn| - scope/transaction<br> - scope/transaction-coordinator|Transaction
test|- scope/test|Code tests
ci|- scope/ci|CI workflow changes or debugging
build|- scope/build| - Dependency (Maven) <br> - Docker <br> - Build or release script
misc|- scope/misc| Changes that do not belong to any scopes above.
doc|- doc|Documentation
site<br>(website)|- website|Website

#### Client

The following changes are shown on the client release notes. 

`scope` and PR label must be one of the following.

scope | Pulsar PR label | Where do you make changes?
|---|---|---
client<br>(Java client)|scope/client-java|Java client
ws<br>(WebSocket)|scope/client-websocket|[WebSocket API](https://pulsar.apache.org/docs/next/client-libraries-websocket/)

### `Summary`

`Summary` is a single line that best sums up the changes made in the commit. 

Follow the best practice below.

* Keep the summary concise and descriptive.
  
* Use the second person and present tense.  
  
* Write [complete sentences](https://www.grammarly.com/blog/sentence-fragment/#:~:text=What's%20a%20sentence%20fragment%3F,%2C%20a%20verb%2C%20or%20both.) rather than fragments.
  
* Capitalize the first letter. 
  
* No period at the end. ❌
  
* Do not include back quotes (``).
  
* Limit the length to 50 characters.
  
* If you cherry pick changes to branches, name your PR title the same as the original PR title and label your PR with cherry-pick related labels.
  
* Do not use [GitHub keywords](https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue#linking-a-pull-request-to-an-issue-using-a-keyword) followed by a #&lt;issue-number>. This information should be provided in PR descriptions or commit messages rather than in PR titles. ❌

### Full examples 

As explained in the [How to write good PR titles](#how-to-write-good-pr-titles) chapter:

> 💡 **Rule**
>
> A good title = clear format ([type](#type) and [scope](#scope)) + self-explanatory [summary](#summary)

Here are some format examples. For self-explanatory summary examples, see [Quick examples](#quick-examples).

Changes|Unclear format ❌|Clear format ✅
---|---|---
Submit breaking changes|[Breaking change] xxx|[feat][broker]! Support xx
Submit PIP changes|[PIP-198] Support xx|[feat][broker] PIP-198: Support xx
Cherry pick changes|[Branch-2.9] Fix xxx issue. | [fix][broker][branch-2.9] Fix xxx issue 
Revert changes|Revert xxx| [revert][broker] Revert changes about xxx
Add features| - Adding xx feature <br> - Support delete schema forcefully| - [feat][java client] Add xx feature <br> - [feat][schema] Support xx
Fix bugs | [Issue 14633][pulsar-broker] Fixed xxx| [fix][broker] Fix xxx 
Submit improvements|- Enhances xx <br> - Bump netty version to 4.1.75 | - [improve][sql] Improve xx performance <br> - [improve][build] Bump Netty version to 4.1.75 
Update tests | reduce xx test flakiness | [improve][test] Reduce xxx flaky tests
Update docs| - [Doc] add explanations for xxx <br> - 2.8.3 Release Notes <br> - Fix typos in xx | - [feat][doc] Add explanations for xxx <br> - [feat][doc] Add 2.8.3 release note <br> - [fix][doc] Fix typos in xx
Update website | [Website] adjust xxx | [improve][site] Adjust xxx
Update instructions/guidelines|Update xxx guideline|[improve][doc] Update xx guidelines

## References

For more guides on how to make contributions to Pulsar docs, see [Pulsar Documentation Contribution Overview](./../README.md).