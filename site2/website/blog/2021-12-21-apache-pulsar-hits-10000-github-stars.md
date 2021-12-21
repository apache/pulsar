---
author: Alice Bi
title: Apache Pulsar Hits 10,000 GitHub Stars
---

Apache Pulsar hits 10,000 GitHub stars. Developers use GitHub stars to show their support for projects and to bookmark projects they want to follow. It is an important measurement to track the engagement of an open source project. The Pulsar community would like to thank every stargazer for joining us in the journey. More importantly, thank you to every Pulsar user, contributor, and committer for making this happen!

Pulsar was initially developed as a cloud-native distributed messaging system at Yahoo! in 2012. Over the past decade, Pulsar has evolved into a unified messaging and streaming platform for event-driven enterprises at scale. In this blog, we look at Pulsar’s community growth, project updates, ecosystem developments, and what’s next for the project. 

# Community Growth

Pulsar’s success depends on its community. As shown in the chart below, the growth of Pulsar’s GitHub stars accelerated after Pulsar became a top-level Apache Software Foundation project. 

![](https://i.imgur.com/kwxz2XT.png)

The number of contributors overtime is another metric for measuring community engagement. The chart below shows that the number of Pulsar contributors accelerated when Pulsar became a top-level Apache project and the growth rate has continued into 2021. 

![](https://i.imgur.com/TYy4CQg.png)

[Pulsar surpassed Kafka in the number of monthly active contributors in early 2021](https://www.apiseven.com/en/contributor-graph?chart=contributorMonthlyActivity&repo=apache/pulsar,apache/kafka). This shows that the development and engagement of Pulsar has grown rapidly over the past few years. 

![](https://i.imgur.com/6AN3dtM.png)

# Project Updates and Ecosystem Development

Recent Pulsar project updates have brought new capabilities to the project. Below we look at key releases and launches.  

## 1. Pulsar 2.8 - Unified Messaging and Streaming with Transactions

Pulsar 2.8 (released in June 2021) introduced many major updates. The Pulsar Transaction API was added to support atomicity across multiple topics and enable end-to-end exactly-once message delivery guarantee for streaming jobs. Replicated subscriptions was added to enhance Pulsar’s geo-replication. With this feature, a consumer can restart consuming from the failure point in a different cluster in case of failover. 

## 2. Function Mesh - Simplifying Complex Streaming Jobs in the Cloud

Function Mesh is an ideal tool for those who are seeking cloud-native serverless streaming solutions. It is a Kubernetes operator that enables users to run Pulsar Functions and connectors natively on Kubernetes, unlocking the full power of Kubernetes’ application deployment, scaling, and management. Function Mesh is also a serverless framework used to orchestrate multiple Pulsar Functions and I/O connectors for complex streaming jobs in a simple way.

## 3. Pulsar Connectors - AWS SQS Connector, Cloud Storage Sink Connector, and More

Pulsar connectors enable easy integration between Pulsar and external systems with added benefits. For instance, the AWS SQS Connector enables secure integration between Pulsar and SQS without needing to write any code. And the Cloud Storage Sink Connector can export data by guaranteeing exactly-once delivery semantics to its consumers. It provides applications that export data from Pulsar, the benefits of fault tolerance, parallelism, elasticity, and much more. 

## 4. Protocol Handlers - Kaffa-on-Pulsar, MQTT-on-Pulsar, and More

Protocol handlers, such as Kaffa-on-Pulsar (KoP) and MQTT-on-Pulsar, allow Pulsar to interact with applications built on other messaging platforms, lowering the barrier to Pulsar adoption. KoP became production-ready since KoP 2.8 and MoP enables MQTT applications to leverage Pulsar’s infinite event stream retention with BookKeeper and tiered storage.

# What’s Next

In the upcoming Pulsar 2.9 release, you can expect the following updates: 

* Introducing a pluggable metadata interface for ZooKeeper metadata management to improve consistency, resilience, and stability, and reduce technical debt. 
* Launching the Oracle Debezium Connector and the schema-aware Elasticsearch Sink Connector.
* Adding the ability to run Kafka Connect sinks as Pulsar sinks.

The Pulsar community is planning more features for future Pulsar releases, including removing ZooKeeper, autoscaling topics, and simplified management of document-based policy. 

# Get Involved

* Pulsar 2.8.1 was released in September 2021. [Download it now](https://pulsar.apache.org/en/download/) and try it out! 
* Join the [Pulsar community on Slack](https://apache-pulsar.herokuapp.com/).

