---
author: Jonathan Ellis
authorURL: https://twitter.com/spyced
title: Five Minute Interview: Apache Pulsar with Elastic
---

This is the first in a series of quick interviews with members of the Apache Pulsar community to learn more about how Pulsar is being used in the wild.  Our guest today is [Ricardo Ferreira](https://twitter.com/riferrei).

<!--truncate-->

### What industry do you work in and what is your role?

I work in the Technology/SaaS space. I work for Elastic, the company behind the Elastic Stack and Elastic Cloud. There, I am part of the community team focused on North America, working as a developer advocate.

### What projects / use cases led to you adopting Pulsar?

I have a background working with messaging and streaming data technologies. Whether working as Developer Advocate for Confluent (where my main focus was Kafka) or as a Solution Architect for Oracle I was constantly presented with opportunities to help customers to implement data pipelines using concepts of streaming and fast data, to replace batch oriented systems that are no longer relevant in today's economy where decisions must be taken instantly. When I was first introduced to Pulsar nearly 1.5 years ago I was impressed with its simplicity which led me to start digging deeper into the technology so I could expand my knowledge.

Most recently I have been helping developers to adopt Pulsar in conjunction with Elasticsearch and Kibana — to allow them to implement end-2-end analytics solutions based on streaming data. Another use case that I have been helping developers with is on how to instrument their apps that write/read data streams to/from Pulsar to generate telemetry data compatible with the OpenTelemetry specification to Elastic APM.

### How did you decide that Pulsar was the right technology to adopt? Were there specific features you wanted to take advantage of?

I don't really believe in this concept of "right technology" without a proper context. Developers in need of a streaming data technology that may be evaluating Pulsar as an option should come up with their own reasoning. I believe that Pulsar has some unique capabilities that if relevant in the right context may make Pulsar look good when compared to others.

I personally love the Pulsar APIs and how simple they are in terms of making the coding simpler and fun. The fact that Pulsar is both a streaming and a messaging technology also pleases me. It is not uncommon to find scenarios where you need both in the same application and Pulsar delivers it beautifully. Arguably this is what "simplification of IT" should be about.

The storage system of Pulsar also solves the very critical problem of handling more load without necessarily adding more compute capacity. You can easily increase just the storage which is cheaper.

Finally, Pulsar provides a more simpler ecosystem to manage. Most of the components that are necessary in a real data pipeline application (such as schema registries, connectors, transformers) are available OOTB. While physically they are still different instances of a given deployment, they are not necessarily foreign technologies. Ultimately everything is all Pulsar brokers with different roles.

### What was the most useful resource or content to you as you got started?

The Apache Pulsar website and its documentation was pretty much all I had to read in order to get started with the technology and getting things done. Occasionally I had to visit the project's [source code on GitHub](https://github.com/apache/pulsar) to understand certain behaviors (such as how to proper parse a topic name to get info about tenant, namespace, etc) and also the [community available on Slack](https://pulsar.apache.org/contact/). People there are friendly and knowledgeable.

### What advice would you have for other organizations who are considering Pulsar?

I would say, "try to remember that Pulsar is a distributed system". While this may sound obvious at first, it is incredible how many people forget about this and try to adopt certain technologies without putting this in check. Distributed systems are inherently complex to manage and though Pulsar simplifies a lot the operations side of things it is still a complex beast that needs to be managed — which makes things harder if you don't have (or don't want to have) this skill in-house. Luckily we have managed services that can alleviate a lot of this pain and if you're a company that is not in the technology business — you better take managed services for granted.

### Beyond your current use case, what are some additional use cases/projects where you would like to use Pulsar in your organization in the future?

As part of the OpenTelemetry project which aims to equip developers with APIs and SDKs to generate telemetry data, it would be nice to see Pulsar as an exporter option. Exporters in the context of OpenTelemetry are components that carry telemetry data to backend systems — likely telemetry backends such as Elastic APM. Having Pulsar as the "buffer" layer for architectures like this would be great, if not awesome. Kafka is currently the only streaming data technology with a proper exporter and it is good to have options.

### If you could wave a magic wand and change one thing about Pulsar, what would it be?

I would use it to transform Pulsar Functions into a full fetched stream processing framework. Currently Pulsar Functions implement very basic use cases related to stream processing, and are far away from becoming as mature as frameworks such as Apache Flink. If that was a heck of a magic wand then I would use it to make [Apache Flink](https://flink.apache.org/) part of the Apache Pulsar project!
