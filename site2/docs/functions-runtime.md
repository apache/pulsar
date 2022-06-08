---
id: functions-runtime
title: Configure Functions runtime
sidebar_label: "Configure Functions runtime"
---

You can configure a specific [function runtime](functions-concepts.md#function-runtime) for your functions.

The following table outlines the supported programming languages for each type of function runtime.

| Function runtime					                          | Supported programming languages of functions |
| —---------------------------------------------------|----------------------------------------------|
| [Thread runtime](functions-runtime-thread)      		| Java 				                                 |
| [Process runtime](functions-runtime-process)      	| Java, Python, Go			                       |
| [Kubernetes runtime](functions-runtime-kubernetes)	| Java, Python, Go		                         |

:::note

For the runtime Java version, refer to [Pulsar Runtime Java Version Recommendation](https://github.com/apache/pulsar/blob/master/README.md#pulsar-runtime-java-version-recommendation) according to your target Pulsar version.

:::
