---
id: functions-develop-schema-registry
title: Use schema registry
sidebar_label: "Use schema registry"
---

Pulsar has a built-in schema registry and is bundled with popular schema types, such as Avro, JSON and Protobuf. Pulsar Functions can leverage the existing schema information from input topics and derive the input type. The schema registry applies to output topics as well.

For more details, refer to [code example](https://github.com/apache/pulsar/blob/master/pulsar-functions/java-examples/src/main/java/org/apache/pulsar/functions/api/examples/AutoSchemaFunction.java).
