---
id: pulsar-api-overview
title: Pulsar API
sidebar_label: "Overview"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````

Pulsar is a messaging and streaming platform that scales across organizations of all sizes. Pulsar APIs are the core building blocks of Pulsar, which allow you to interact with Pulsar or administer Pulsar clusters. Pulsar APIs consist of the following types: 

- [Pulsar client APIs](client-api-overview.md)

- [Pulsar admin APIs](admin-api-overview.md)

![Pulsar APIs - Definition](/assets/pulsar-api-definition.svg)

## When to use Pulsar APIs

Pulsar client APIs and Pulsar admin APIs serve two different purposes in the Pulsar API design. You can use Pulsar client APIs to build applications with Pulsar and use Pulsar admin APIs to manage Pulsar clusters.

Here is a simple comparison between Pulsar client APIs and Pulsar admin APIs.

Category|Pulsar client APIs|Pulsar admin APIs
---|---|---|
Audiences|Developers|DevOps
Goals|Build applications with Pulsar|Administer Pulsar clusters
Use cases|Pulsar client APIs help you create applications that rely on real-time data. <br/><br/> For example, you can build a financial application to handle fraud alerts or an eCommerce application that creates recommendations based on user activities.| Pulsar administration APIs let you administer the entire Pulsar instance, including clusters, tenants, namespaces, and topics, from a single endpoint. <br/><br/> For example, you can configure security and compliance, or get information about brokers, check for any issues, and then troubleshoot solutions.
Key features|- Process data with producers, consumers, readers, and TableView <br/><br/> - Secure data with authentication and authorization <br/><br/> - Protect data with transactions and schema <br/><br/> - Stabilize data with cluster-level auto failover | - Configure authentication and authorization <br/><br/> - Set data retention and resource isolation policies <br/><br/> - Facilitate workflow of application development<br/><br/> - Troubleshoot Pulsar
Interfaces | - [Java client API](client-libraries-java.md) <br/><br/> - [C++ client API](client-libraries-cpp.md) <br/><br/> - [Python client API](client-libraries-python.md) <br/><br/> -  [Go client API](client-libraries-go.md) <br/><br/> - [Node.js client API](client-libraries-node.md) <br/><br/> - [WebSocket client API](client-libraries-websocket.md) <br/><br/> - [C# client API](client-libraries-dotnet.md) | - [Java admin API](admin-api-overview.md) <br/><br/> - [REST API](reference-rest-api-overview.md)

