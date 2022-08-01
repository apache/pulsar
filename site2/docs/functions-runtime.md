---
id: functions-runtime
title: Configure function runtime
sidebar_label: "Configure function runtime"
---

Pulsar supports three types of [function runtime](functions-concepts.md#function-runtime) with different costs and isolation guarantees to maximize deployment flexibility of your functions.

The following table outlines the supported programming languages for each type of function runtime.

| Function runtime                                   | Supported programming languages of functions |
|----------------------------------------------------|----------------------------------------------|
| [Thread runtime](functions-runtime-thread.md)         | Java                                         |
| [Process runtime](functions-runtime-process.md)       | Java, Python, Go                             |
| [Kubernetes runtime](functions-runtime-kubernetes.md) | Java, Python, Go                             |

:::note

For the runtime Java version, refer to [Pulsar Runtime Java Version Recommendation](https://github.com/apache/pulsar/blob/master/README.md#pulsar-runtime-java-version-recommendation) according to your target Pulsar version.

:::
